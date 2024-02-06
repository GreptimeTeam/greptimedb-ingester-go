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

func (c *StreamClient) Send(ctx context.Context, tables ...*table.Table) error {
	header_ := header.New(c.cfg.Database).WithAuth(c.cfg.Username, c.cfg.Password)
	request_, err := request.New(header_, tables...).Build()
	if err != nil {
		return err
	}
	return c.client.Send(request_)
}

func (c *StreamClient) Write(ctx context.Context, body any) error {
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
