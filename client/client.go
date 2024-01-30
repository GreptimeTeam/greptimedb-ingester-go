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
	"fmt"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"google.golang.org/grpc"

	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
	"github.com/GreptimeTeam/greptimedb-ingester-go/request"
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
	conn, err := grpc.Dial(cfg.GetGRPCAddr(), cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	client := greptimepb.NewGreptimeDatabaseClient(conn)
	return &Client{cfg: cfg, client: client}, nil
}

func (c *Client) Write(ctx context.Context, tables ...*table.Table) (*greptimepb.GreptimeResponse, error) {
	req, err := request.New(tables...).Build(c.cfg)
	fmt.Printf("dbname: %#v\n", req.Header.GetDbname())

	inserts := req.GetRowInserts().GetInserts()

	for _, insert := range inserts {
		fmt.Printf("table name: %q\n", insert.GetTableName())

		fmt.Println("columns:")
		for _, schema := range insert.Rows.GetSchema() {
			fmt.Printf("name: %#v\n", schema.GetColumnName())
			fmt.Printf("semantic: %#v\n", schema.GetSemanticType())
			fmt.Printf("type: %#v\n", schema.GetDatatype())
		}

		fmt.Println()
		fmt.Println()
		fmt.Println("rows:")

		for _, row := range insert.GetRows().GetRows() {
			for _, val := range row.GetValues() {
				fmt.Printf("value: %#v\n", val.String())
			}
			fmt.Println()
			fmt.Println()
		}

		fmt.Println()

	}
	if err != nil {
		return nil, err
	}
	return c.client.Handle(ctx, req, c.cfg.CallOptions...)
}
