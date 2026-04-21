/*
 * Copyright 2023 Greptime Team
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
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	"github.com/GreptimeTeam/greptimedb-ingester-go/internal/pool"
	"github.com/GreptimeTeam/greptimedb-ingester-go/loadbalancer"
	"github.com/GreptimeTeam/greptimedb-ingester-go/request"
	"github.com/GreptimeTeam/greptimedb-ingester-go/request/header"
	"github.com/GreptimeTeam/greptimedb-ingester-go/schema"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

// Client helps to write data into GreptimeDB. A Client is safe for concurrent
// use by multiple goroutines; one instance per application is typical.
//
// When configured with multiple endpoints via Config.WithEndpoints, unary
// calls (Write, Delete, HealthCheck, BulkWrite) pick an endpoint per call
// through the configured loadbalancer.Picker. A streaming session opened by
// StreamWrite/StreamDelete binds to a single endpoint until CloseStream;
// if that endpoint fails mid-stream, the Send returns the underlying error
// and the next StreamWrite call picks a fresh endpoint to open a new stream.
type Client struct {
	cfg  *Config
	pool *pool.Pool

	// closed guards Close against repeated invocations. The pool pointer
	// itself is intentionally left stable after Close so that any RPC still
	// in flight — or a post-close misuse — surfaces as a gRPC transport
	// error rather than a nil-pointer dereference.
	closed atomic.Bool

	streamMu sync.Mutex
	stream   gpb.GreptimeDatabase_HandleRequestsClient
}

// NewClient creates the greptimedb client responsible for writing data into
// GreptimeDB. Endpoints configured via WithEndpoints take precedence over
// Host:Port; each entry must be a "host:port" string.
func NewClient(cfg *Config) (*Client, error) {
	addrs := cfg.resolveEndpoints()
	if len(addrs) == 0 {
		return nil, fmt.Errorf("greptime: no endpoint configured; pass a host to NewConfig or call WithEndpoints")
	}
	for _, addr := range addrs {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("greptime: invalid endpoint %q: %w", addr, err)
		}
	}

	picker := cfg.picker
	if picker == nil {
		picker = loadbalancer.NewRandom()
	}

	p, err := pool.New(addrs, picker, cfg.build())
	if err != nil {
		return nil, err
	}

	return &Client{cfg: cfg, pool: p}, nil
}

// submit builds a request and dispatches it to a picked endpoint. picking
// fresh per call keeps the path retry-friendly once a retry layer lands.
func (c *Client) submit(ctx context.Context, operation types.Operation, tables ...*table.Table) (*gpb.GreptimeResponse, error) {
	header_ := header.New(c.cfg.Database).WithAuth(c.cfg.Username, c.cfg.Password)
	request_, err := request.New(header_, operation, tables...).Build()
	if err != nil {
		return nil, err
	}
	return c.pool.Pick().DB.Handle(ctx, request_)
}

// Write is to write the data into GreptimeDB via explicit schema.
//
//	tbl, err := table.New(<tableName>)
//
//	// add column at first. This is to define the schema of the table.
//	tbl.AddTagColumn("tag1", types.INT64)
//	tbl.AddFieldColumn("field1", types.STRING)
//	tbl.AddFieldColumn("field2", types.FLOAT64)
//	tbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MILLISECOND)
//	timestamp := time.Now()
//	// you can add multiple row(s). This is the real data.
//	tbl.AddRow(1, "hello", 1.1, timestamp)
//
//	// write data into GreptimeDB
//	resp, err := client.Write(context.Background(), tbl)
func (c *Client) Write(ctx context.Context, tables ...*table.Table) (*gpb.GreptimeResponse, error) {
	return c.submit(ctx, types.INSERT, tables...)
}

// Delete is to delete the data from GreptimeDB via explicit schema.
//
//	tbl, err := table.New(<tableName>)
//
//	// add column at first. This is to define the schema of the table.
//	tbl.AddTagColumn("tag1", types.INT64)
//	tbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MILLISECOND)
//
//	// you can add multiple row(s). This is the real data.
//	tbl.AddRow("tag1", timestamp)
//
//	// delete the data from GreptimeDB
//	resp, err := client.Delete(context.Background() tbl)
func (c *Client) Delete(ctx context.Context, tables ...*table.Table) (*gpb.GreptimeResponse, error) {
	return c.submit(ctx, types.DELETE, tables...)
}

// WriteObject is like [Write] to write the data into GreptimeDB, but schema is defined in the struct tag.
//
//	type Monitor struct {
//	  ID          int64     `greptime:"tag;column:id;type:int64"`
//	  Host        string    `greptime:"tag;column:host;type:string"`
//	  Memory      uint64    `greptime:"field;column:memory;type:uint64"`
//	  Cpu         float64   `greptime:"field;column:cpu;type:float64"`
//	  Temperature int64     `greptime:"field;column:temperature;type:int64"`
//	  Running     bool      `greptime:"field;column:running;type:boolean"`
//	  Ts          time.Time `greptime:"timestamp;column:ts;type:timestamp;precision:millisecond"`
//	}
//
//	func (Monitor) TableName() string {
//	  return monitorTableName
//	}
//
//	monitors := []Monitor{
//		{
//		    ID:          randomId(),
//		    Host:        "127.0.0.1",
//		    Memory:      1,
//		    Cpu:         1.0,
//		    Temperature: -1,
//		    Ts:          time1,
//		    Running:     true,
//		},
//		{
//		    ID:          randomId(),
//		    Host:        "127.0.0.2",
//		    Memory:      2,
//		    Cpu:         2.0,
//		    Temperature: -2,
//		    Ts:          time2,
//		    Running:     true,
//		},
//	}
//
//	resp, err := client.WriteObject(context.Background(), monitors)
func (c *Client) WriteObject(ctx context.Context, obj any) (*gpb.GreptimeResponse, error) {
	tbl, err := schema.Parse(obj)
	if err != nil {
		return nil, err
	}

	return c.submit(ctx, types.INSERT, tbl)
}

// DeleteObject is like [Delete] to delete the data from GreptimeDB, but schema is defined in the struct tag.
// resp, err := client.DeleteObject(context.Background(), deleteMonitors)
func (c *Client) DeleteObject(ctx context.Context, obj any) (*gpb.GreptimeResponse, error) {
	tbl, err := schema.Parse(obj)
	if err != nil {
		return nil, err
	}

	return c.submit(ctx, types.DELETE, tbl)
}

// streamSubmit lazily opens a bidirectional stream against a picked endpoint
// and sends the request on it. The stream is cached until CloseStream or
// until a Send error is surfaced; on error we clear the cache so that the
// next call re-picks and rebuilds (matches the TypeScript ingester and
// avoids silent failover that could gap or duplicate rows).
//
// The mutex serializes stream construction (gRPC Send is not itself safe for
// concurrent calls) and also fixes a pre-existing TOCTOU race on the cached
// stream.
func (c *Client) streamSubmit(ctx context.Context, operation types.Operation, tables ...*table.Table) error {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()

	if c.stream == nil {
		s, err := c.pool.Pick().DB.HandleRequests(ctx)
		if err != nil {
			return err
		}
		c.stream = s
	}

	header_ := header.New(c.cfg.Database).WithAuth(c.cfg.Username, c.cfg.Password)
	request_, err := request.New(header_, operation, tables...).Build()
	if err != nil {
		return err
	}
	if err := c.stream.Send(request_); err != nil {
		// Clear the broken stream so the next call rebuilds on a fresh pick.
		c.stream = nil
		return err
	}
	return nil
}

// StreamWrite is to send the data into GreptimeDB via explicit schema.
//
//	tbl, err := table.New(<tableName>)
//
//	// add column at first. This is to define the schema of the table.
//	tbl.AddTagColumn("tag1", types.INT64)
//	tbl.AddFieldColumn("field1", types.STRING)
//	tbl.AddFieldColumn("field2", types.FLOAT64)
//	tbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MILLISECOND)
//	timestamp = time.Now()
//	// you can add multiple row(s). This is the real data.
//	tbl.AddRow(1, "hello", 1.1, timestamp)
//
//	// send data into GreptimeDB
//	resp, err := client.StreamWrite(context.Background(), tbl)
func (c *Client) StreamWrite(ctx context.Context, tables ...*table.Table) error {
	return c.streamSubmit(ctx, types.INSERT, tables...)
}

// StreamDelete is to delete the data from GreptimeDB via explicit schema.
//
//	tbl, err := table.New(<tableName>)
//
//	// add column at first. This is to define the schema of the table.
//	tbl.AddTagColumn("tag1", types.INT64)
//	tbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MILLISECOND)
//
//	// you can add multiple row(s). This is the real data.
//	tbl.AddRow("tag1", timestamp)
//
//	// delete the data from GreptimeDB
//	resp, err := client.StreamDelete(context.Background(), tbl)
func (c *Client) StreamDelete(ctx context.Context, tables ...*table.Table) error {
	return c.streamSubmit(ctx, types.DELETE, tables...)
}

// StreamWriteObject is like [StreamWrite] to send the data into GreptimeDB, but schema is defined in the struct tag.
//
//	type monitor struct {
//	  ID          int64     `greptime:"tag;column:id;type:int64"`
//	  Host        string    `greptime:"tag;column:host;type:string"`
//	  Memory      uint64    `greptime:"field;column:memory;type:uint64"`
//	  Cpu         float64   `greptime:"field;column:cpu;type:float64"`
//	  Temperature int64     `greptime:"field;column:temperature;type:int64"`
//	  Running     bool      `greptime:"field;column:running;type:boolean"`
//	  Ts          time.Time `greptime:"timestamp;column:ts;type:timestamp;precision:millisecond"`
//	}
//
//	func (monitor) TableName() string {
//	  return monitorTableName
//	}
//
//	monitors := []monitor{
//		{
//		    ID:          randomId(),
//		    Host:        "127.0.0.1",
//		    Memory:      1,
//		    Cpu:         1.0,
//		    Temperature: -1,
//		    Ts:          time1,
//		    Running:     true,
//		},
//		{
//		    ID:          randomId(),
//		    Host:        "127.0.0.2",
//		    Memory:      2,
//		    Cpu:         2.0,
//		    Temperature: -2,
//		    Ts:          time2,
//		    Running:     true,
//		},
//	}
//
//	resp, err := client.StreamWriteObject(context.Background(), monitors)
func (c *Client) StreamWriteObject(ctx context.Context, body any) error {
	tbl, err := schema.Parse(body)
	if err != nil {
		return err
	}
	return c.streamSubmit(ctx, types.INSERT, tbl)
}

// StreamDeleteObject is like [StreamDelete] to Delete the data from GreptimeDB, but schema is defined in the struct tag.
// resp, err := client.StreamDeleteObject(context.Background(), deleteMonitors)
func (c *Client) StreamDeleteObject(ctx context.Context, body any) error {
	tbl, err := schema.Parse(body)
	if err != nil {
		return err
	}
	return c.streamSubmit(ctx, types.DELETE, tbl)
}

// CloseStream closes the stream. Once we’ve finished writing our client’s requests to the stream
// using client.StreamWrite or client.StreamWriteObject, we need to call client.CloseStream to let
// GreptimeDB know that we’ve finished writing and are expecting to receive a response.
func (c *Client) CloseStream(_ context.Context) (*gpb.AffectedRows, error) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()

	if c.stream == nil {
		return &gpb.AffectedRows{}, nil
	}

	resp, err := c.stream.CloseAndRecv()
	c.stream = nil
	if err != nil {
		return nil, err
	}

	return resp.GetAffectedRows(), nil
}

// HealthCheck will check GreptimeDB health status. With multiple endpoints
// configured, HealthCheck probes whichever endpoint the picker returns for
// this call, not every endpoint.
func (c *Client) HealthCheck(ctx context.Context) (*gpb.HealthCheckResponse, error) {
	req := &gpb.HealthCheckRequest{}
	return c.pool.Pick().Health.HealthCheck(ctx, req)
}

// Close terminates all underlying gRPC connections. Any active stream is
// aborted by the connection teardown; callers that need a graceful
// half-close must call CloseStream first. Call this method when the
// client is no longer needed. Close is idempotent; RPCs issued after
// Close fail with a gRPC transport error rather than panicking.
func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	c.streamMu.Lock()
	c.stream = nil
	c.streamMu.Unlock()

	return c.pool.Close()
}

// BulkWrite performs a high-efficiency bulk data write operation to GreptimeDB using Apache Arrow format.
// It sends the entire table data in a single batch, which is more efficient for large datasets compared to row-by-row writes.
// The table must have columns and rows properly defined before calling this method.
func (c *Client) BulkWrite(ctx context.Context, table *table.Table) (*gpb.GreptimeResponse, error) {
	return c.pool.Pick().Bulk.BulkWrite(ctx, table)
}
