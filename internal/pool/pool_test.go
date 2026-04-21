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

package pool

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/GreptimeTeam/greptimedb-ingester-go/loadbalancer"
)

// fakeHealthServer counts HealthCheck invocations so tests can assert
// which endpoint received a given call.
type fakeHealthServer struct {
	gpb.UnimplementedHealthCheckServer
	calls atomic.Uint64
}

func (s *fakeHealthServer) HealthCheck(_ context.Context, _ *gpb.HealthCheckRequest) (*gpb.HealthCheckResponse, error) {
	s.calls.Add(1)
	return &gpb.HealthCheckResponse{}, nil
}

type fakeEndpoint struct {
	addr   string
	server *grpc.Server
	health *fakeHealthServer
}

func startFakeEndpoint(t *testing.T) *fakeEndpoint {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	health := &fakeHealthServer{}
	srv := grpc.NewServer()
	gpb.RegisterHealthCheckServer(srv, health)

	go func() { _ = srv.Serve(lis) }()

	return &fakeEndpoint{
		addr:   lis.Addr().String(),
		server: srv,
		health: health,
	}
}

func (e *fakeEndpoint) stop() { e.server.Stop() }

func dialOpts() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
}

func TestPoolRoundRobinDispatch(t *testing.T) {
	const n = 3
	eps := make([]*fakeEndpoint, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		eps[i] = startFakeEndpoint(t)
		addrs[i] = eps[i].addr
	}
	defer func() {
		for _, e := range eps {
			e.stop()
		}
	}()

	p, err := New(addrs, loadbalancer.NewRoundRobin(), dialOpts())
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	const iters = 30
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < iters; i++ {
		_, err := p.Pick().Health.HealthCheck(ctx, &gpb.HealthCheckRequest{})
		require.NoError(t, err)
	}

	for i, e := range eps {
		assert.Equal(t, uint64(iters/n), e.health.calls.Load(),
			"endpoint %d (%s) call count", i, e.addr)
	}
}

func TestPoolRandomHitsAllEndpoints(t *testing.T) {
	const n = 3
	eps := make([]*fakeEndpoint, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		eps[i] = startFakeEndpoint(t)
		addrs[i] = eps[i].addr
	}
	defer func() {
		for _, e := range eps {
			e.stop()
		}
	}()

	p, err := New(addrs, loadbalancer.NewRandom(), dialOpts())
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 300; i++ {
		_, err := p.Pick().Health.HealthCheck(ctx, &gpb.HealthCheckRequest{})
		require.NoError(t, err)
	}
	for i, e := range eps {
		assert.Greater(t, e.health.calls.Load(), uint64(0),
			"endpoint %d (%s) should have received at least one call", i, e.addr)
	}
}

func TestPoolSingleEndpointSkipsPicker(t *testing.T) {
	ep := startFakeEndpoint(t)
	defer ep.stop()

	// Use a picker that would explode if called, proving the fast path bypass.
	p, err := New([]string{ep.addr}, panicPicker{}, dialOpts())
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = p.Pick().Health.HealthCheck(ctx, &gpb.HealthCheckRequest{})
	require.NoError(t, err)
}

func TestPoolDuplicateAddrsDeduplicated(t *testing.T) {
	ep := startFakeEndpoint(t)
	defer ep.stop()

	p, err := New([]string{ep.addr, ep.addr, ep.addr}, loadbalancer.NewRoundRobin(), dialOpts())
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	assert.Equal(t, []string{ep.addr}, p.Addrs())
}

func TestPoolEmptyAddrsError(t *testing.T) {
	_, err := New(nil, loadbalancer.NewRandom(), dialOpts())
	assert.Error(t, err)
}

func TestPoolNilPickerError(t *testing.T) {
	_, err := New([]string{"127.0.0.1:1"}, nil, dialOpts())
	assert.Error(t, err)
}

func TestPoolCloseIsIdempotent(t *testing.T) {
	ep := startFakeEndpoint(t)
	defer ep.stop()

	p, err := New([]string{ep.addr}, loadbalancer.NewRandom(), dialOpts())
	require.NoError(t, err)

	assert.NoError(t, p.Close())
	// Second Close on already-closed gRPC conns should be non-fatal; gRPC
	// returns an error but the pool joins and returns it. We only assert it
	// does not panic.
	_ = p.Close()
}

type panicPicker struct{}

func (panicPicker) Pick(_ []string) string { panic("picker should not be called for single endpoint") }
