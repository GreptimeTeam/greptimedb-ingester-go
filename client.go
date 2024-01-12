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

package greptime

import (
	"context"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"google.golang.org/grpc"

	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
)

// Client helps to Insert/Query data Into/From GreptimeDB. A Client is safe for concurrent
// use by multiple goroutines,you can have one Client instance in your application.
type Client struct {
	cfg *config.Config

	// For `query`, since unary calls have not been implemented for query and only do_get helps
	flightClient flight.Client

	// For `insert`, unary calls are supported
	greptimeClient greptimepb.GreptimeDatabaseClient

	// For `Promql` query
	promqlClient greptimepb.PrometheusGatewayClient
}

// NewClient helps to create the greptimedb client, which will be responsible Write/Read data To/From GreptimeDB
func NewClient(cfg *config.Config) (*Client, error) {
	flightClient, err := flight.NewClientWithMiddleware(cfg.GetGRPCAddr(), nil, nil, cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(cfg.GetGRPCAddr(), cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	greptimeClient := greptimepb.NewGreptimeDatabaseClient(conn)
	promqlClient := greptimepb.NewPrometheusGatewayClient(conn)

	return &Client{
		cfg:            cfg,
		flightClient:   flightClient,
		greptimeClient: greptimeClient,
		promqlClient:   promqlClient,
	}, nil
}

// Insert helps to insert multiple rows of multiple tables into greptimedb
func (c *Client) Insert(ctx context.Context, req InsertsRequest) (*greptimepb.GreptimeResponse, error) {
	request, err := req.build(c.cfg)
	if err != nil {
		return nil, err
	}
	return c.greptimeClient.Handle(ctx, request, c.cfg.CallOptions...)
}
