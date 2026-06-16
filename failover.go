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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GreptimeTeam/greptimedb-ingester-go/loadbalancer"
)

// RetryPolicy controls how unary calls (Write, Delete, HealthCheck) are retried
// across endpoints on retryable transport failures. Streaming and bulk calls
// are not auto-retried; see the package README.
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

// retryableCodes are transport-level gRPC status codes worth retrying on
// another endpoint. Mirrors the TypeScript ingester's conservative set.
var retryableCodes = map[codes.Code]struct{}{
	codes.Unknown:           {},
	codes.DeadlineExceeded:  {},
	codes.ResourceExhausted: {},
	codes.Aborted:           {},
	codes.Unavailable:       {},
}

// endpointFailureCodes indicate the endpoint itself is unhealthy (connectivity
// or capacity), so a health-aware selector should temporarily avoid it. A
// server business error — even a retryable one — means the endpoint is alive
// and routing correctly and must NOT eject it.
var endpointFailureCodes = map[codes.Code]struct{}{
	codes.DeadlineExceeded:  {},
	codes.ResourceExhausted: {},
	codes.Unavailable:       {},
}

// isRetryable reports whether err is a transient transport failure worth
// retrying on another endpoint. Caller-initiated cancellation never retries.
func isRetryable(err error) bool {
	if err == nil || isCancellation(err) {
		return false
	}
	_, ok := retryableCodes[status.Code(err)]
	return ok
}

// isEndpointFailure reports whether err indicates the endpoint itself is
// unhealthy and should be temporarily avoided by a health-aware selector.
func isEndpointFailure(err error) bool {
	if err == nil || isCancellation(err) {
		return false
	}
	_, ok := endpointFailureCodes[status.Code(err)]
	return ok
}

// isCancellation reports whether err is a caller-initiated cancellation, which
// says nothing about endpoint health and must never be retried.
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
			if lastErr != nil {
				return zero, lastErr
			}
			return zero, err
		}

		peer := selector.Select(addrs, failed)
		res, err := call(peer)
		reportOutcome(health, peer, err)
		if err == nil {
			return res, nil
		}

		failed[peer] = struct{}{}
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
		return ctx.Err()
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
