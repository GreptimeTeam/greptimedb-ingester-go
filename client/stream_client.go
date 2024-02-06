// Copyright 2024 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"google.golang.org/grpc"

	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
	"github.com/GreptimeTeam/greptimedb-ingester-go/request"
	"github.com/GreptimeTeam/greptimedb-ingester-go/request/header"
	"github.com/GreptimeTeam/greptimedb-ingester-go/schema"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
)

// StreamClient is only for inserting
type StreamClient struct {
	cfg *config.Config

	client gpb.GreptimeDatabase_HandleRequestsClient
}

func NewStreamClient(cfg *config.Config) (*StreamClient, error) {
	conn, err := grpc.Dial(cfg.GetEndpoint(), cfg.Options().Build()...)
	if err != nil {
		return nil, err
	}

	client, err := gpb.NewGreptimeDatabaseClient(conn).HandleRequests(context.Background())
	if err != nil {
		return nil, err
	}

	return &StreamClient{client: client, cfg: cfg}, nil
}

// Send is to send the data into GreptimeDB via explicit schema.
//
//	    tbl := table.New(<tableName>)
//
//		// add column at first. This is to define the schema of the table.
//		tbl.AddTagColumn("tag1", types.INT64)
//		tbl.AddFieldColumn("field1", types.STRING)
//		tbl.AddFieldColumn("field2", types.DOUBLE)
//		tbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MILLISECONDS)
//
//		// you can add multiple row(s). This is the real data.
//		tbl.AddRow(1, "hello", 1.1, time.Now())
//
//		// send data into GreptimeDB
//		resp, err := streamClient.Send(context.Background(), tbl)
func (c *StreamClient) Send(ctx context.Context, tables ...*table.Table) error {
	header_ := header.New(c.cfg.Database).WithAuth(c.cfg.Username, c.cfg.Password)
	request_, err := request.New(header_, tables...).Build()
	if err != nil {
		return err
	}
	return c.client.Send(request_)
}

// Create is like [Send] to send the data into GreptimeDB, but schema is defined in the struct tag.
//
//		type monitor struct {
//		  ID          int64     `greptime:"tag;column:id;type:int64"`
//		  Host        string    `greptime:"tag;column:host;type:string"`
//		  Memory      uint64    `greptime:"field;column:memory;type:uint64"`
//		  Cpu         float64   `greptime:"field;column:cpu;type:float64"`
//		  Temperature int64     `greptime:"field;column:temperature;type:int64"`
//		  Running     bool      `greptime:"field;column:running;type:boolean"`
//		  Ts          time.Time `greptime:"timestamp;column:ts;type:timestamp;precision:millisecond"`
//		}
//
//		func (monitor) TableName() string {
//		  return monitorTableName
//		}
//
//		monitors := []monitor{
//			{
//			    ID:          randomId(),
//			    Host:        "127.0.0.1",
//			    Memory:      1,
//			    Cpu:         1.0,
//			    Temperature: -1,
//			    Ts:          time1,
//			    Running:     true,
//			},
//			{
//			    ID:          randomId(),
//			    Host:        "127.0.0.2",
//			    Memory:      2,
//			    Cpu:         2.0,
//			    Temperature: -2,
//			    Ts:          time2,
//			    Running:     true,
//			},
//	  }
//
//		resp, err := streamClient.Create(context.Background(), monitors)
func (c *StreamClient) Create(ctx context.Context, body any) error {
	tbl, err := schema.Parse(body)
	if err != nil {
		return err
	}
	return c.Send(ctx, tbl)
}

func (c *StreamClient) CloseAndRecv(ctx context.Context) (*gpb.AffectedRows, error) {
	resp, err := c.client.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	return resp.GetAffectedRows(), nil
}
