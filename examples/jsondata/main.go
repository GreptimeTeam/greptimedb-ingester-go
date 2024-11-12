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
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

const (
	// The GreptimeDB address.
	host = "127.0.0.1"

	// The database name.
	database = "public"
)

type Person struct {
	Name      string
	Age       int
	IsStudent bool
	Courses   []string
}

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

func main() {
	c, err := newClient()
	if err != nil {
		log.Fatalf("failed to new client: %v:", err)
	}

	data, err := initData()
	if err != nil {
		log.Fatalf("failed to init data: %v:", err)
	}
	if err = c.write(data[0]); err != nil {
		log.Fatalf("failed to write data: %v:", err)
	}
}

func initData() ([]*table.Table, error) {
	time1 := time.Now()

	itbl, err := table.New("json_data")
	if err != nil {
		return nil, err
	}

	p := Person{
		Name:      "Jain Doe",
		Age:       25,
		IsStudent: false,
		Courses:   []string{"math", "history", "chemistry"},
	}

	// add column at first. This is to define the schema of the table.
	if err := itbl.AddFieldColumn("my_json", types.JSON); err != nil {
		return nil, err
	}
	if err := itbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		return nil, err
	}
	if err := itbl.AddRow(p, time1); err != nil {
		return nil, err
	}

	return []*table.Table{itbl}, nil
}

func (c client) write(data *table.Table) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := c.client.Write(ctx, data)
	if err != nil {
		return err
	}
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
	return nil
}
