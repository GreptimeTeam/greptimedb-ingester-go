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

// Package pool manages a set of gRPC connections to GreptimeDB endpoints and
// dispatches calls to one of them through a pluggable loadbalancer.Picker.
package pool

import (
	"errors"
	"fmt"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"google.golang.org/grpc"

	"github.com/GreptimeTeam/greptimedb-ingester-go/bulk"
	"github.com/GreptimeTeam/greptimedb-ingester-go/loadbalancer"
)

// Endpoint bundles a gRPC connection with the stubs created from it. All
// stubs on one Endpoint share the same underlying *grpc.ClientConn.
type Endpoint struct {
	Addr   string
	Conn   *grpc.ClientConn
	DB     gpb.GreptimeDatabaseClient
	Health gpb.HealthCheckClient
	Bulk   *bulk.BulkClient
}

// Pool holds one Endpoint per address and dispatches Pick calls through the
// configured Picker. Pool is safe for concurrent use.
type Pool struct {
	addrs     []string
	endpoints map[string]*Endpoint
	picker    loadbalancer.Picker
}

// New dials every address with the given dial options and returns a Pool.
// grpc.NewClient is non-blocking: bad addresses surface as errors at the
// first RPC, not here.
func New(addrs []string, picker loadbalancer.Picker, dialOpts []grpc.DialOption) (*Pool, error) {
	if len(addrs) == 0 {
		return nil, errors.New("pool: at least one endpoint is required")
	}
	if picker == nil {
		return nil, errors.New("pool: picker must not be nil")
	}

	p := &Pool{
		addrs:     make([]string, 0, len(addrs)),
		endpoints: make(map[string]*Endpoint, len(addrs)),
		picker:    picker,
	}

	for _, addr := range addrs {
		if _, dup := p.endpoints[addr]; dup {
			// Duplicates would skew the picker; skip silently to keep the
			// surface forgiving, but don't open two conns to the same host.
			continue
		}
		conn, err := grpc.NewClient(addr, dialOpts...)
		if err != nil {
			// Undo partial state before returning.
			_ = p.Close()
			return nil, fmt.Errorf("pool: dial %s: %w", addr, err)
		}
		p.endpoints[addr] = &Endpoint{
			Addr:   addr,
			Conn:   conn,
			DB:     gpb.NewGreptimeDatabaseClient(conn),
			Health: gpb.NewHealthCheckClient(conn),
			Bulk:   bulk.NewBulkClient(conn),
		}
		p.addrs = append(p.addrs, addr)
	}
	return p, nil
}

// Pick returns one Endpoint chosen by the configured picker. For a single
// endpoint the picker is bypassed to avoid any per-call overhead.
func (p *Pool) Pick() *Endpoint {
	if len(p.addrs) == 1 {
		return p.endpoints[p.addrs[0]]
	}
	return p.endpoints[p.picker.Pick(p.addrs)]
}

// Addrs returns the list of endpoint addresses. The returned slice must not
// be mutated.
func (p *Pool) Addrs() []string {
	return p.addrs
}

// Close closes every underlying gRPC connection. Errors from individual
// closes are joined.
func (p *Pool) Close() error {
	var errs []error
	for _, ep := range p.endpoints {
		if ep.Conn != nil {
			if err := ep.Conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close %s: %w", ep.Addr, err))
			}
		}
	}
	return errors.Join(errs...)
}
