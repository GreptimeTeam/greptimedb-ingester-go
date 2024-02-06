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

// Client helps to write data into GreptimeDB. A Client is safe for concurrent
// use by multiple goroutines,you can have one Client instance in your application.
type Client struct {
	cfg *config.Config

	client gpb.GreptimeDatabaseClient
}

// New helps to create the greptimedb client, which will be responsible write data into GreptimeDB.
func New(cfg *config.Config) (*Client, error) {
	conn, err := grpc.Dial(cfg.GetEndpoint(), cfg.Options().Build()...)
	if err != nil {
		return nil, err
	}

	client := gpb.NewGreptimeDatabaseClient(conn)
	return &Client{cfg: cfg, client: client}, nil
}

// Write is to write the data into GreptimeDB via explicit schema.
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
//		// write data into GreptimeDB
//		resp, err := client.Write(context.Background(), tbl)
func (c *Client) Write(ctx context.Context, tables ...*table.Table) (*gpb.GreptimeResponse, error) {
	header_ := header.New(c.cfg.Database).WithAuth(c.cfg.Username, c.cfg.Password)
	request_, err := request.New(header_, tables...).Build()
	if err != nil {
		return nil, err
	}
	return c.client.Handle(ctx, request_)
}

func (c *Client) Create(ctx context.Context, body any) (*gpb.GreptimeResponse, error) {
	tbl, err := schema.Parse(body)
	if err != nil {
		return nil, err
	}

	return c.Write(ctx, tbl)
}
