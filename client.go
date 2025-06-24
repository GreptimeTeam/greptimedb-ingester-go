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

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"google.golang.org/grpc"

	"github.com/GreptimeTeam/greptimedb-ingester-go/request"
	"github.com/GreptimeTeam/greptimedb-ingester-go/request/header"
	"github.com/GreptimeTeam/greptimedb-ingester-go/schema"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

// Client helps to write data into GreptimeDB. A Client is safe for concurrent
// use by multiple goroutines,you can have one Client instance in your application.
type Client struct {
	cfg               *Config
	conn              *grpc.ClientConn
	client            gpb.GreptimeDatabaseClient
	stream            gpb.GreptimeDatabase_HandleRequestsClient
	healthCheckClient gpb.HealthCheckClient
}

// NewClient helps to create the greptimedb client, which will be responsible write data into GreptimeDB.
func NewClient(cfg *Config) (*Client, error) {
	conn, err := grpc.NewClient(cfg.endpoint(), cfg.build()...)
	if err != nil {
		return nil, err
	}

	client := gpb.NewGreptimeDatabaseClient(conn)
	healthCheckClient := gpb.NewHealthCheckClient(conn)

	return &Client{
		cfg:               cfg,
		client:            client,
		conn:              conn,
		healthCheckClient: healthCheckClient,
	}, nil
}

// submit is to build request and send it to GreptimeDB.
// The operations can be set:
//   - INSERT
//   - DELETE
func (c *Client) submit(ctx context.Context, operation types.Operation, tables ...*table.Table) (*gpb.GreptimeResponse, error) {
	header_ := header.New(c.cfg.Database).WithAuth(c.cfg.Username, c.cfg.Password)
	request_, err := request.New(header_, operation, tables...).Build()
	if err != nil {
		return nil, err
	}
	return c.client.Handle(ctx, request_)
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

// streamSubmit is to build stream request and send it to GreptimeDB.
// The operations can be set:
//   - INSERT
//   - DELETE
func (c *Client) streamSubmit(ctx context.Context, operation types.Operation, tables ...*table.Table) error {
	if c.stream == nil {
		stream, err := c.client.HandleRequests(ctx)
		if err != nil {
			return err
		}
		c.stream = stream
	}

	header_ := header.New(c.cfg.Database).WithAuth(c.cfg.Username, c.cfg.Password)
	request_, err := request.New(header_, operation, tables...).Build()
	if err != nil {
		return err
	}
	return c.stream.Send(request_)
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
func (c *Client) CloseStream(ctx context.Context) (*gpb.AffectedRows, error) {
	if c.stream == nil {
		return &gpb.AffectedRows{}, nil
	}

	resp, err := c.stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	c.stream = nil
	return resp.GetAffectedRows(), nil
}

// HealthCheck will check GreptimeDB health status.
func (c *Client) HealthCheck(ctx context.Context) (*gpb.HealthCheckResponse, error) {
	req := &gpb.HealthCheckRequest{}
	resp, err := c.healthCheckClient.HealthCheck(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// Close terminates the gRPC connection.
// Call this method when the client is no longer needed.
func (c *Client) Close() error {
	// Close the stream if it's open
	if c.stream != nil {
		_, err := c.stream.CloseAndRecv()
		if err != nil {
			return err
		}
		c.stream = nil
	}

	// Close the connection
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			return err
		}
		c.conn = nil
	}

	return nil
}
