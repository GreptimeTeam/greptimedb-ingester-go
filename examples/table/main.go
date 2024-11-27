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
	INSERT = 0
	UPDATE = 1
	DELETE = 2

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

func initData() []*table.Table {

	time1 := time.Now()
	time2 := time.Now()
	time3 := time.Now()

	itbl, err := table.New("monitors_with_schema")
	if err != nil {
		log.Fatal(err)
	}
	// add column at first. This is to define the schema of the table.
	if err := itbl.AddTagColumn("id", types.INT64); err != nil {
		log.Fatal(err)
	}
	if err := itbl.AddFieldColumn("host", types.STRING); err != nil {
		log.Fatal(err)
	}
	if err := itbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		log.Fatal(err)
	}
	if err := itbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		log.Fatal(err)
	}

	if err := itbl.AddRow(1, "hello", 1.1, time1); err != nil {
		log.Fatal(err)
	}
	if err := itbl.AddRow(2, "hello", 2.2, time2); err != nil {
		log.Fatal(err)
	}
	if err := itbl.AddRow(3, "hello", 3.3, time3); err != nil {
		log.Fatal(err)
	}

	utbl, err := table.New("monitors_with_schema")
	if err != nil {
		log.Fatal(err)
	}

	// add column at first. This is to define the schema of the table.
	if err := utbl.AddTagColumn("id", types.INT64); err != nil {
		log.Fatal(err)
	}
	if err := utbl.AddFieldColumn("host", types.STRING); err != nil {
		log.Fatal(err)
	}
	if err := utbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		log.Fatal(err)
	}
	if err := utbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		log.Fatal(err)
	}

	if err := utbl.AddRow(1, "hello", 1.2, time1); err != nil {
		log.Fatal(err)
	}

	dtbl, err := table.New("monitors_with_schema")
	if err != nil {
		log.Fatal(err)
	}

	// add column at first. This is to define the schema of the table.
	if err := dtbl.AddTagColumn("id", types.INT64); err != nil {
		log.Fatal(err)
	}
	if err := dtbl.AddFieldColumn("host", types.STRING); err != nil {
		log.Fatal(err)
	}
	if err := dtbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		log.Fatal(err)
	}
	if err := dtbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		log.Fatal(err)
	}

	if err := dtbl.AddRow(3, "hello", 3.3, time3); err != nil {
		log.Fatal(err)
	}

	return []*table.Table{itbl, utbl, dtbl}
}

func (c *client) write(data *table.Table) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := c.client.Write(ctx, data)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
}

func (c *client) delete(data *table.Table) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := c.client.Delete(ctx, data)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
}

func (c *client) streamWrite(data *table.Table) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	if err := c.client.StreamWrite(ctx, data); err != nil {
		log.Fatal(err)
	}
	affected, err := c.client.CloseStream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("affected rows: %d\n", affected.GetValue())
}

func (c *client) streamDelete(data *table.Table) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	if err := c.client.StreamDelete(ctx, data); err != nil {
		log.Fatal(err)
	}
	affected, err := c.client.CloseStream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("affected rows: %d\n", affected.GetValue())
}

func main() {
	c, err := newClient()
	if err != nil {
		log.Fatalf("failed to new client: %v", err)
	}

	data := initData()
	// insert
	c.write(data[INSERT])
	// update
	c.write(data[UPDATE])
	// delete
	c.delete(data[DELETE])

	time.Sleep(time.Millisecond * 100)

	data = initData()
	// stream insert
	c.streamWrite(data[INSERT])
	// stream update
	c.streamWrite(data[UPDATE])
	// stream delete
	c.streamDelete(data[DELETE])

}
