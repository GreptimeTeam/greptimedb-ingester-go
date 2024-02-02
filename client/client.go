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

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"google.golang.org/grpc"

	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
	"github.com/GreptimeTeam/greptimedb-ingester-go/request"
	"github.com/GreptimeTeam/greptimedb-ingester-go/request/header"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
)

// Client helps to write data into GreptimeDB. A Client is safe for concurrent
// use by multiple goroutines,you can have one Client instance in your application.
type Client struct {
	cfg *config.Config

	client greptimepb.GreptimeDatabaseClient
}

// New helps to create the greptimedb client, which will be responsible write data into GreptimeDB.
func New(cfg *config.Config) (*Client, error) {
	conn, err := grpc.Dial(cfg.GetEndpoint(), cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	client := greptimepb.NewGreptimeDatabaseClient(conn)
	return &Client{cfg: cfg, client: client}, nil
}

func (c *Client) Write(ctx context.Context, tables ...*table.Table) (*greptimepb.GreptimeResponse, error) {
	header_ := header.New().WithDatabase(c.cfg.Database).WithAuth(c.cfg.Username, c.cfg.Password)
	request_, err := request.New().WithTables(tables...).WithHeader(header_).Build()
	if err != nil {
		return nil, err
	}
	return c.client.Handle(ctx, request_, c.cfg.CallOptions...)
}
