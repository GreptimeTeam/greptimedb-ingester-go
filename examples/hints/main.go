// Copyright 2024 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"log"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"

	"github.com/GreptimeTeam/greptimedb-ingester-go/pkg/hints"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

const (
	INSERT = 0

	// The GreptimeDB address.
	host = "127.0.0.1"

	// The database name.
	database = "public"
)

type client struct {
	client *greptime.Client
}

func newClient() (*client, error) {
	cfg := greptime.NewConfig(host).WithDatabase(database)
	gtClient, err := greptime.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	c := &client{
		client: gtClient,
	}

	return c, nil
}

func initData() ([]*table.Table, error) {
	time1 := time.Now()
	time2 := time.Now()
	time3 := time.Now()

	itbl, err := table.New("monitor_table_with_hints")
	if err != nil {
		return nil, err
	}
	// add column at first. This is to define the schema of the table.
	if err := itbl.AddTagColumn("id", types.INT64); err != nil {
		return nil, err
	}
	if err := itbl.AddFieldColumn("host", types.STRING); err != nil {
		return nil, err
	}
	if err := itbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		return nil, err
	}
	if err := itbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		return nil, err
	}

	if err := itbl.AddRow(1, "hello", 1.1, time1); err != nil {
		return nil, err
	}
	if err := itbl.AddRow(2, "hello", 2.2, time2); err != nil {
		return nil, err
	}
	if err := itbl.AddRow(3, "hello", 3.3, time3); err != nil {
		return nil, err
	}

	return []*table.Table{itbl}, nil
}

func (c client) write(data *table.Table) error {
	var h []hints.Hint
	h = append(h,
		hints.Hint{Key: "ttl", Value: "3d"},
		hints.Hint{Key: "merge_mode", Value: "last_non_null"},
		hints.Hint{Key: "append_mode", Value: "false"},
	)

	ctx := hints.CreateContextWithHints(h)
	ctx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()

	resp, err := c.client.Write(ctx, data)
	if err != nil {
		return err
	}
	tableName, err := data.GetName()
	if err != nil {
		return err
	}

	log.Printf("create table, name: '%s'", tableName)
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
	return nil
}

func main() {
	data, err := initData()
	if err != nil {
		log.Fatalf("failed to init data: %v:", err)
	}

	c, err := newClient()
	if err != nil {
		log.Fatalf("failed to new client: %v:", err)
	}

	err = c.write(data[INSERT])
	if err != nil {
		log.Fatalf("failed to write data: %v:", err)
	}
}
