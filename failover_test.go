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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recordingSelector records every Select call and its exclude snapshot, plus
// the health-hook calls. It returns the first non-excluded endpoint (falling
// open to the first endpoint), so exclusion is observable. It implements both
// loadbalancer.Selector and loadbalancer.HealthReporter.
type recordingSelector struct {
	selects   []selectCall
	successes []string
	failures  []string
}

type selectCall struct {
	peer    string
	exclude []string
}

func (s *recordingSelector) Pick(endpoints []string) string {
	return s.Select(endpoints, nil)
}

func (s *recordingSelector) Select(endpoints []string, exclude map[string]struct{}) string {
	peer := endpoints[0]
	for _, ep := range endpoints {
		if _, skip := exclude[ep]; !skip {
			peer = ep
			break
		}
	}
	keys := make([]string, 0, len(exclude))
	for ep := range exclude {
		keys = append(keys, ep)
	}
	s.selects = append(s.selects, selectCall{peer: peer, exclude: keys})
	return peer
}

func (s *recordingSelector) ReportSuccess(endpoint string) {
	s.successes = append(s.successes, endpoint)
}
func (s *recordingSelector) ReportFailure(endpoint string) { s.failures = append(s.failures, endpoint) }

var testAddrs = []string{"h1:4001", "h2:4001"}

func fastRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2}
}

func unavailable() error { return status.Error(codes.Unavailable, "down") }

func TestFailoverExcludesFailedPeersThenSucceeds(t *testing.T) {
	sel := &recordingSelector{}
	errs := []error{unavailable(), unavailable(), nil}
	var calls int

	res, err := runWithFailover(context.Background(), testAddrs, sel, sel, fastRetry(),
		func(peer string) (int, error) {
			e := errs[calls]
			calls++
			if e != nil {
				return 0, e
			}
			return 7, nil
		})

	require.NoError(t, err)
	assert.Equal(t, 7, res)
	require.Len(t, sel.selects, 3)
	assert.Empty(t, sel.selects[0].exclude)
	assert.Contains(t, sel.selects[1].exclude, "h1:4001")
	assert.ElementsMatch(t, []string{"h1:4001", "h2:4001"}, sel.selects[2].exclude)
}

func TestFailoverReportsEndpointFailureButNotBusinessError(t *testing.T) {
	sel := &recordingSelector{}
	// First: transport failure → endpoint failure. Second: a business error
	// (the endpoint answered, so it is alive) → must not eject. Third: success.
	errs := []error{unavailable(), status.Error(codes.InvalidArgument, "bad"), nil}
	var calls int

	_, err := runWithFailover(context.Background(), testAddrs, sel, sel, fastRetry(),
		func(peer string) (int, error) {
			e := errs[calls]
			calls++
			return 0, e
		})

	// The InvalidArgument is non-retryable, so the loop stops at attempt 2.
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Equal(t, []string{"h1:4001"}, sel.failures)
	assert.Contains(t, sel.successes, "h2:4001")
}

func TestFailoverStopsOnNonRetryableError(t *testing.T) {
	sel := &recordingSelector{}
	var calls int

	_, err := runWithFailover(context.Background(), testAddrs, sel, sel, fastRetry(),
		func(peer string) (int, error) {
			calls++
			return 0, status.Error(codes.InvalidArgument, "bad")
		})

	require.Error(t, err)
	assert.Equal(t, 1, calls, "non-retryable error must not retry")
	require.Len(t, sel.selects, 1)
	// A business error proves the endpoint is alive — reported as success.
	assert.Empty(t, sel.failures)
	assert.Equal(t, []string{"h1:4001"}, sel.successes)
}

func TestFailoverExhaustsAttemptsAndReturnsLastError(t *testing.T) {
	sel := &recordingSelector{}
	var calls int

	_, err := runWithFailover(context.Background(), testAddrs, sel, sel, fastRetry(),
		func(peer string) (int, error) {
			calls++
			return 0, unavailable()
		})

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, 3, calls, "should try MaxAttempts times")
	assert.Len(t, sel.failures, 3)
}

func TestFailoverNoHealthReporterStillExcludes(t *testing.T) {
	sel := &recordingSelector{}
	errs := []error{unavailable(), nil}
	var calls int

	// Pass nil health: stateless picker case. Exclude steering must still work.
	res, err := runWithFailover(context.Background(), testAddrs, sel, nil, fastRetry(),
		func(peer string) (int, error) {
			e := errs[calls]
			calls++
			if e != nil {
				return 0, e
			}
			return 1, nil
		})

	require.NoError(t, err)
	assert.Equal(t, 1, res)
	assert.Empty(t, sel.successes, "nil health reporter records nothing")
	assert.Empty(t, sel.failures)
	assert.Contains(t, sel.selects[1].exclude, "h1:4001")
}

func TestFailoverHonorsCancelledContext(t *testing.T) {
	sel := &recordingSelector{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := runWithFailover(ctx, testAddrs, sel, sel, fastRetry(),
		func(peer string) (int, error) { return 1, nil })

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, sel.selects, "must not attempt with a cancelled context")
}

func TestFailoverCancellationDuringBackoffStops(t *testing.T) {
	sel := &recordingSelector{}
	ctx, cancel := context.WithCancel(context.Background())
	policy := RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Hour, MaxBackoff: time.Hour, BackoffMultiplier: 2}

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err := runWithFailover(ctx, testAddrs, sel, sel, policy,
		func(peer string) (int, error) { return 0, unavailable() })

	require.ErrorIs(t, err, context.Canceled)
}

func TestIsRetryable(t *testing.T) {
	assert.True(t, isRetryable(status.Error(codes.Unavailable, "")))
	assert.True(t, isRetryable(status.Error(codes.DeadlineExceeded, "")))
	assert.True(t, isRetryable(status.Error(codes.ResourceExhausted, "")))
	assert.True(t, isRetryable(status.Error(codes.Aborted, "")))
	assert.True(t, isRetryable(status.Error(codes.Unknown, "")))

	assert.False(t, isRetryable(nil))
	assert.False(t, isRetryable(status.Error(codes.InvalidArgument, "")))
	assert.False(t, isRetryable(status.Error(codes.NotFound, "")))
	assert.False(t, isRetryable(status.Error(codes.Canceled, "")))
	assert.False(t, isRetryable(context.Canceled))
}

func TestIsEndpointFailure(t *testing.T) {
	assert.True(t, isEndpointFailure(status.Error(codes.Unavailable, "")))
	assert.True(t, isEndpointFailure(status.Error(codes.DeadlineExceeded, "")))
	assert.True(t, isEndpointFailure(status.Error(codes.ResourceExhausted, "")))

	// Retryable transient but not an endpoint-health signal.
	assert.False(t, isEndpointFailure(status.Error(codes.Aborted, "")))
	assert.False(t, isEndpointFailure(status.Error(codes.Unknown, "")))
	// Business errors: the endpoint answered.
	assert.False(t, isEndpointFailure(status.Error(codes.InvalidArgument, "")))
	assert.False(t, isEndpointFailure(nil))
	assert.False(t, isEndpointFailure(context.Canceled))
}

func TestComputeBackoffCapsAndGrows(t *testing.T) {
	policy := RetryPolicy{InitialBackoff: 100 * time.Millisecond, MaxBackoff: 500 * time.Millisecond, BackoffMultiplier: 2}
	assert.Equal(t, 100*time.Millisecond, computeBackoff(policy, 0))
	assert.Equal(t, 200*time.Millisecond, computeBackoff(policy, 1))
	assert.Equal(t, 400*time.Millisecond, computeBackoff(policy, 2))
	// 100 * 2^3 = 800 > 500 cap.
	assert.Equal(t, 500*time.Millisecond, computeBackoff(policy, 3))
}

func TestComputeBackoffFullJitterWithinBounds(t *testing.T) {
	policy := RetryPolicy{InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second, BackoffMultiplier: 2, Jitter: true}
	for i := 0; i < 100; i++ {
		d := computeBackoff(policy, 1) // base 200ms
		assert.GreaterOrEqual(t, d, time.Duration(0))
		assert.LessOrEqual(t, d, 200*time.Millisecond)
	}
}

// Sanity: the wrapper produced by errors.Is on a wrapped cancellation is treated
// as a cancellation, not a retryable transport failure.
func TestWrappedCancellationNotRetryable(t *testing.T) {
	wrapped := errors.Join(errors.New("ctx"), context.Canceled)
	assert.True(t, isCancellation(wrapped))
	assert.False(t, isRetryable(wrapped))
}
