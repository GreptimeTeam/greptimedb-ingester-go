/*
 * Copyright 2025 Greptime Team
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

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

const (
	host     = "127.0.0.1"
	port     = 4001
	database = "public"
)

type client struct {
	client *greptime.Client
}

func newClient() (*client, error) {
	cfg := greptime.NewConfig(host).
		WithPort(port).
		WithDatabase(database)

	c, err := greptime.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Greptime client: %w", err)
	}

	return &client{
		client: c,
	}, nil
}

func initData() *table.Table {
	now := time.Now()

	itbl, err := table.New("bulk_insert_table")
	if err != nil {
		log.Fatal(err)
	}

	if err := itbl.AddTagColumn("id", types.INT64); err != nil {
		log.Fatal(err)
	}
	if err := itbl.AddFieldColumn("host", types.STRING); err != nil {
		log.Fatal(err)
	}
	if err := itbl.AddFieldColumn("temperature", types.FLOAT64); err != nil {
		log.Fatal(err)
	}
	if err := itbl.AddTimestampColumn("ts", types.TIMESTAMP_MICROSECOND); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := itbl.AddRow(
			int64(i),
			fmt.Sprintf("host-%d", i),
			20.5+float64(i),
			now.Add(time.Duration(i)*time.Second).UnixMicro(),
		); err != nil {
			log.Fatal(err)
		}
	}

	return itbl
}

func main() {
	c, err := newClient()
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.client.Close()

	data := initData()
	log.Printf("Writing %d rows to table", len(data.GetRows().Rows))

	response, err := c.client.BulkWrite(context.TODO(), data)
	if err != nil {
		log.Fatalf("BulkWrite failed after retries: %v", err)
	}

	if response.GetAffectedRows() != nil {
		fmt.Printf("Write successful! Affected rows: %d\n", response.GetAffectedRows().Value)
	} else {
		fmt.Println("Write successful but no affected rows information")
	}
}
