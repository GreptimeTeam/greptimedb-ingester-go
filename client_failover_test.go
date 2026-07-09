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
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/GreptimeTeam/greptimedb-ingester-go/loadbalancer"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

// fakeDBServer is a minimal GreptimeDatabase backend whose Handle behavior is
// switchable, so tests can simulate a healthy endpoint and an unavailable one.
type fakeDBServer struct {
	gpb.UnimplementedGreptimeDatabaseServer
	addr      string
	server    *grpc.Server
	calls     atomic.Uint64
	available atomic.Bool
	// bizCode, when nonzero, makes Handle return a business error carrying this
	// GreptimeDB status code in the x-greptime-err-code trailer.
	bizCode atomic.Uint32
}

func (s *fakeDBServer) Handle(ctx context.Context, _ *gpb.GreptimeRequest) (*gpb.GreptimeResponse, error) {
	s.calls.Add(1)
	// Simulate a GreptimeDB business error: a gRPC status plus the precise
	// status code in the x-greptime-err-code trailer, exactly as the server does.
	if code := s.bizCode.Load(); code != 0 {
		_ = grpc.SetTrailer(ctx, metadata.Pairs("x-greptime-err-code", strconv.FormatUint(uint64(code), 10)))
		return nil, status.Error(codes.ResourceExhausted, "business error")
	}
	if !s.available.Load() {
		return nil, status.Error(codes.Unavailable, "endpoint down")
	}
	return &gpb.GreptimeResponse{
		Response: &gpb.GreptimeResponse_AffectedRows{
			AffectedRows: &gpb.AffectedRows{Value: 1},
		},
	}, nil
}

func startFakeDB(t *testing.T, available bool) *fakeDBServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := &fakeDBServer{addr: lis.Addr().String(), server: grpc.NewServer()}
	s.available.Store(available)
	gpb.RegisterGreptimeDatabaseServer(s.server, s)
	go func() { _ = s.server.Serve(lis) }()
	t.Cleanup(s.server.Stop)
	return s
}

func failoverTestTable(t *testing.T) *table.Table {
	t.Helper()
	tbl, err := table.New("failover_demo")
	require.NoError(t, err)
	require.NoError(t, tbl.AddTagColumn("host", types.STRING))
	require.NoError(t, tbl.AddFieldColumn("cpu", types.FLOAT64))
	require.NoError(t, tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND))
	require.NoError(t, tbl.AddRow("h", 1.0, time.Now()))
	return tbl
}

// TestClientWriteFailsOverToHealthyEndpoint exercises the full public path:
// a write whose first endpoint is unavailable must succeed on the other one.
func TestClientWriteFailsOverToHealthyEndpoint(t *testing.T) {
	down := startFakeDB(t, false)
	up := startFakeDB(t, true)

	cfg := NewConfig().
		WithDatabase("public").
		WithEndpoints(down.addr, up.addr).
		WithLoadBalancer(loadbalancer.NewRoundRobin()).
		WithRetry(RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2})
	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Several writes: regardless of which endpoint round-robin tries first,
	// every write must ultimately land on the healthy endpoint.
	for i := 0; i < 6; i++ {
		resp, err := client.Write(ctx, failoverTestTable(t))
		require.NoError(t, err, "write %d should fail over to the healthy endpoint", i)
		assert.Equal(t, uint32(1), resp.GetAffectedRows().GetValue())
	}
	assert.GreaterOrEqual(t, up.calls.Load(), uint64(6), "healthy endpoint served every write")
}

// TestClientWriteEjectsAndRecoversEndpoint verifies a health-aware selector
// ejects a failing endpoint from rotation and re-admits it once it recovers.
func TestClientWriteEjectsAndRecoversEndpoint(t *testing.T) {
	down := startFakeDB(t, false)
	up := startFakeDB(t, true)

	detector := loadbalancer.NewOutlierDetector(loadbalancer.OutlierDetectorOptions{
		Base:                loadbalancer.NewRoundRobin(),
		ConsecutiveFailures: 1, // eject on first transport failure
		BaseEjection:        time.Hour,
	})
	cfg := NewConfig().
		WithDatabase("public").
		WithEndpoints(down.addr, up.addr).
		WithLoadBalancer(detector).
		WithRetry(RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2})
	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Drive enough writes that the bad endpoint gets selected and ejected.
	for i := 0; i < 6; i++ {
		_, err := client.Write(ctx, failoverTestTable(t))
		require.NoError(t, err)
	}

	// Once ejected (BaseEjection is an hour), the bad endpoint stops receiving
	// calls. Record its count, run more writes, and assert it did not grow.
	ejectedCount := down.calls.Load()
	for i := 0; i < 6; i++ {
		_, err := client.Write(ctx, failoverTestTable(t))
		require.NoError(t, err)
	}
	assert.Equal(t, ejectedCount, down.calls.Load(),
		"ejected endpoint must not be selected while ejected")

	// Recovery: the endpoint comes back and a fresh success re-admits it. We
	// report success directly to model an out-of-band health probe; subsequent
	// selection should include it again.
	down.available.Store(true)
	detector.ReportSuccess(down.addr)
	got := map[string]struct{}{}
	for i := 0; i < 50; i++ {
		got[detector.Select([]string{down.addr, up.addr}, nil)] = struct{}{}
	}
	assert.Contains(t, got, down.addr, "recovered endpoint should re-enter rotation")
}

// A non-retryable business error (RateLimited maps to ResourceExhausted but is
// not retryable) must fail fast without burning retries, and must not eject the
// endpoint — it answered correctly.
func TestClientWriteDoesNotRetryNonRetryableServerError(t *testing.T) {
	s := startFakeDB(t, true)
	s.bizCode.Store(6001) // RateLimited

	detector := loadbalancer.NewOutlierDetector(loadbalancer.OutlierDetectorOptions{ConsecutiveFailures: 1})
	cfg := NewConfig().
		WithDatabase("public").
		WithEndpoints(s.addr).
		WithLoadBalancer(detector).
		WithRetry(RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2})
	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Write(ctx, failoverTestTable(t))
	require.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "underlying gRPC status preserved")
	assert.Equal(t, uint64(1), s.calls.Load(), "non-retryable server error must not retry")
	// The endpoint answered, so it stays healthy and selectable.
	assert.Equal(t, s.addr, detector.Select([]string{s.addr}, nil))
}

// A retryable business error (RegionBusy) is retried on another endpoint, and
// the busy endpoint is not ejected (it is alive, just transiently busy).
func TestClientWriteRetriesRetryableServerError(t *testing.T) {
	busy := startFakeDB(t, true)
	busy.bizCode.Store(4009) // RegionBusy → retryable
	up := startFakeDB(t, true)

	detector := loadbalancer.NewOutlierDetector(loadbalancer.OutlierDetectorOptions{
		Base:                loadbalancer.NewRoundRobin(),
		ConsecutiveFailures: 1,
	})
	cfg := NewConfig().
		WithDatabase("public").
		WithEndpoints(busy.addr, up.addr).
		WithLoadBalancer(detector).
		WithRetry(RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2})
	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 6; i++ {
		resp, err := client.Write(ctx, failoverTestTable(t))
		require.NoError(t, err, "write %d should retry past the RegionBusy endpoint", i)
		assert.Equal(t, uint32(1), resp.GetAffectedRows().GetValue())
	}
	// Busy endpoint must remain selectable — a business error never ejects it.
	picks := map[string]struct{}{}
	for i := 0; i < 50; i++ {
		picks[detector.Select([]string{busy.addr, up.addr}, nil)] = struct{}{}
	}
	assert.Contains(t, picks, busy.addr, "RegionBusy endpoint must not be ejected")
}
