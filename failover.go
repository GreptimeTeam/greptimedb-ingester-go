/*
 * Copyright 2026 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package greptime

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/GreptimeTeam/greptimedb-ingester-go/loadbalancer"
)

// RetryPolicy controls how unary calls (Write, Delete, HealthCheck) are retried
// across endpoints on retryable transport failures and transient GreptimeDB
// server errors. Streaming and bulk calls are not auto-retried; see the package
// README.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts including the first. Values
	// below 1 are treated as 1 (no retry).
	MaxAttempts int
	// InitialBackoff is the base delay before the first retry.
	InitialBackoff time.Duration
	// MaxBackoff caps the per-retry delay.
	MaxBackoff time.Duration
	// BackoffMultiplier grows the delay each retry
	// (delay = InitialBackoff * BackoffMultiplier^attempt).
	BackoffMultiplier float64
	// Jitter, when true, applies full jitter: the actual delay is uniform in
	// [0, computed]. Recommended to avoid synchronized retries across clients.
	Jitter bool
}

// DefaultRetryPolicy mirrors the TypeScript ingester defaults: three attempts
// with exponential backoff and full jitter.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts:       3,
	InitialBackoff:    100 * time.Millisecond,
	MaxBackoff:        5 * time.Second,
	BackoffMultiplier: 2,
	Jitter:            true,
}

// greptimeErrCodeTrailer is the gRPC trailer key under which GreptimeDB returns
// its business StatusCode on an errored response (mirrors the server-side
// common_error::GREPTIME_DB_HEADER_ERROR_CODE).
const greptimeErrCodeTrailer = "x-greptime-err-code"

// retryableServerStatus is the set of GreptimeDB business status codes that are
// transient and worth retrying. Mirrors StatusCode::is_retryable in GreptimeDB,
// minus Internal (1003): a generic internal error is too often a real bug to
// retry blindly. Same set the TypeScript ingester uses.
//
// These are needed because the server's status_code -> gRPC code mapping is
// lossy: RegionBusy and RateLimited both surface as ResourceExhausted, but only
// RegionBusy is retryable. Classifying on the precise status code restores the
// distinction.
var retryableServerStatus = map[uint32]struct{}{
	4008: {}, // RegionNotReady
	4009: {}, // RegionBusy
	4010: {}, // TableUnavailable
	5000: {}, // StorageUnavailable
	6000: {}, // RuntimeResourcesExhausted
}

// serverStatusError pairs a gRPC status error from GreptimeDB with the business
// StatusCode carried in the x-greptime-err-code trailer. It is unexported and
// transparent: it preserves the underlying gRPC status (GRPCStatus) and error
// chain (Unwrap), so callers still see the original error — it only steers
// retry/health classification internally.
type serverStatusError struct {
	statusCode uint32
	err        error
}

func (e *serverStatusError) Error() string              { return e.err.Error() }
func (e *serverStatusError) Unwrap() error              { return e.err }
func (e *serverStatusError) GRPCStatus() *status.Status { return status.Convert(e.err) }

// withServerStatus annotates err with the GreptimeDB business status code from
// the response trailer when present. A nil error or a trailer without the code
// is returned unchanged.
func withServerStatus(err error, trailer metadata.MD) error {
	if err == nil {
		return nil
	}
	vals := trailer.Get(greptimeErrCodeTrailer)
	if len(vals) == 0 {
		return err
	}
	code, parseErr := strconv.ParseUint(vals[0], 10, 32)
	if parseErr != nil {
		return err
	}
	return &serverStatusError{statusCode: uint32(code), err: err}
}

// serverStatusCode returns the GreptimeDB business status code carried by err,
// if err is a server status error.
func serverStatusCode(err error) (uint32, bool) {
	var se *serverStatusError
	if errors.As(err, &se) {
		return se.statusCode, true
	}
	return 0, false
}

// isRetryable reports whether err is worth retrying on another endpoint.
//
// A GreptimeDB business error is authoritative: only its transient status codes
// retry, regardless of how they collapse onto gRPC codes. Otherwise we fall
// back to the transport-level gRPC codes.
//
// DeadlineExceeded is deliberately NOT retryable: this client forwards the
// caller's context straight to each RPC (there is no per-attempt deadline), so
// a DeadlineExceeded means the caller's own deadline elapsed. Retrying would
// reuse the same expired context and is pointless. The cancellation guard keeps
// a wrapped context.Canceled (which has no gRPC status, so it would otherwise
// classify as Unknown) from being treated as retryable; runWithFailover's own
// context check is the primary stop for caller cancellation.
func isRetryable(err error) bool {
	if isCancellation(err) {
		return false
	}
	if code, ok := serverStatusCode(err); ok {
		_, retry := retryableServerStatus[code]
		return retry
	}
	switch status.Code(err) {
	case codes.Unknown, codes.ResourceExhausted, codes.Aborted, codes.Unavailable:
		return true
	default:
		return false
	}
}

// isEndpointFailure reports whether err indicates the endpoint itself is
// unhealthy (connectivity or capacity) and should be temporarily avoided by a
// health-aware selector.
//
// A server business error — even a retryable one like RegionBusy — means the
// endpoint answered and is routing correctly, so it must NOT eject the endpoint
// (that would punish a healthy frontend for a datanode-side condition).
// DeadlineExceeded is excluded for the same reason as in isRetryable: it
// reflects the caller's clock, not the endpoint's health.
func isEndpointFailure(err error) bool {
	if _, ok := serverStatusCode(err); ok {
		return false
	}
	switch status.Code(err) {
	case codes.ResourceExhausted, codes.Unavailable:
		return true
	default:
		return false
	}
}

// isCancellation reports whether err is a server-returned cancellation, which
// says nothing about endpoint health. Caller-initiated cancellation/deadline is
// detected via the context directly (see runWithFailover), not here.
func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

// runWithFailover executes call across endpoints with bounded retries and
// optional health feedback. It is transport-agnostic — call resolves the peer
// to a concrete RPC — so the loop stays unit-testable without a live server.
//
// On each attempt it asks the selector for a peer, excluding peers that already
// failed in this sequence so a single dead endpoint cannot burn the whole retry
// budget. Outcomes feed the health reporter (when non-nil): a transport-level
// endpoint failure ejects, while a success or a server business error (the
// endpoint answered) clears the streak.
func runWithFailover[T any](
	ctx context.Context,
	addrs []string,
	selector loadbalancer.Selector,
	health loadbalancer.HealthReporter,
	policy RetryPolicy,
	call func(peer string) (T, error),
) (T, error) {
	var zero T
	attempts := policy.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}

	failed := make(map[string]struct{})
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		peer := selector.Select(addrs, failed)
		res, err := call(peer)
		if err == nil {
			reportOutcome(health, peer, nil)
			return res, nil
		}
		// Caller-initiated cancellation or deadline: the endpoint may be
		// perfectly healthy, the caller simply ran out of time. Stop without
		// retrying and without blaming the endpoint's health.
		if ctx.Err() != nil {
			return zero, err
		}

		failed[peer] = struct{}{}
		reportOutcome(health, peer, err)
		lastErr = err
		if attempt == attempts-1 || !isRetryable(err) {
			break
		}
		if werr := sleepBackoff(ctx, policy, attempt); werr != nil {
			return zero, werr
		}
	}
	return zero, lastErr
}

// reportOutcome maps a single endpoint-bound outcome to the selector's health
// hooks. A client-side error never reaches here (request building happens
// before the call), so within the loop an error is always an RPC verdict: a
// transport failure ejects, anything else means the endpoint answered.
func reportOutcome(health loadbalancer.HealthReporter, peer string, err error) {
	if health == nil {
		return
	}
	switch {
	case err == nil:
		health.ReportSuccess(peer)
	case isCancellation(err):
		// Caller-initiated; says nothing about the endpoint.
	case isEndpointFailure(err):
		health.ReportFailure(peer)
	default:
		// The endpoint answered with a business error → it is alive.
		health.ReportSuccess(peer)
	}
}

func sleepBackoff(ctx context.Context, policy RetryPolicy, attempt int) error {
	d := computeBackoff(policy, attempt)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func computeBackoff(policy RetryPolicy, attempt int) time.Duration {
	base := float64(policy.InitialBackoff) * math.Pow(policy.BackoffMultiplier, float64(attempt))
	if maxB := float64(policy.MaxBackoff); maxB > 0 && base > maxB {
		base = maxB
	}
	if policy.Jitter {
		base *= rand.Float64()
	}
	return time.Duration(base)
}
