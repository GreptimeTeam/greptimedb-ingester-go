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

// Package pool maintains one gRPC connection per GreptimeDB endpoint and serves
// the stubs created from it by address. Endpoint selection (load balancing and
// health-aware failover) lives in the client, which drives this registry via
// Addrs and Get.
package pool

import (
	"errors"
	"fmt"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"google.golang.org/grpc"

	"github.com/GreptimeTeam/greptimedb-ingester-go/bulk"
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

// Pool holds one Endpoint per address, addressable by Get. Pool is safe for
// concurrent use: the endpoint map is built once in New and only read afterward.
type Pool struct {
	addrs     []string
	endpoints map[string]*Endpoint
}

// New dials every address with the given dial options and returns a Pool.
// grpc.NewClient is non-blocking: bad addresses surface as errors at the
// first RPC, not here. Duplicate addresses are de-duplicated.
func New(addrs []string, dialOpts []grpc.DialOption) (*Pool, error) {
	if len(addrs) == 0 {
		return nil, errors.New("pool: at least one endpoint is required")
	}

	p := &Pool{
		addrs:     make([]string, 0, len(addrs)),
		endpoints: make(map[string]*Endpoint, len(addrs)),
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

// Get returns the Endpoint for addr, or nil if addr is not in the pool. Callers
// pick addr from Addrs, so a nil result indicates a programming error.
func (p *Pool) Get(addr string) *Endpoint {
	return p.endpoints[addr]
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
