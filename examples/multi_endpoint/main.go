/*
 * Copyright 2026 Greptime Team
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

// Writes to several GreptimeDB endpoints with client-side load balancing.
package main

import (
	"context"
	"log"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/loadbalancer"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

const database = "public"

func main() {
	cfg := greptime.NewConfig().
		WithDatabase(database).
		WithEndpoints("127.0.0.1:4001", "127.0.0.2:4001", "127.0.0.3:4001").
		WithLoadBalancer(loadbalancer.NewRoundRobin()) // default is NewRandom()

	client, err := greptime.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	tbl, err := table.New("multi_endpoint_demo")
	if err != nil {
		log.Fatal(err)
	}
	if err := tbl.AddTagColumn("host", types.STRING); err != nil {
		log.Fatal(err)
	}
	if err := tbl.AddFieldColumn("cpu", types.FLOAT64); err != nil {
		log.Fatal(err)
	}
	if err := tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND); err != nil {
		log.Fatal(err)
	}
	if err := tbl.AddRow("127.0.0.1", 1.0, time.Now()); err != nil {
		log.Fatal(err)
	}

	// Each Write picks one endpoint through the configured load balancer.
	resp, err := client.Write(context.Background(), tbl)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("affected rows: %d", resp.GetAffectedRows().GetValue())
}
