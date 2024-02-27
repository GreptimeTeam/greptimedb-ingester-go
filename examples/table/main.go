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

var (
	client *greptime.Client
)

func init() {
	cfg := greptime.NewConfig("127.0.0.1").WithDatabase("public")

	cli_, err := greptime.NewClient(cfg)
	if err != nil {
		log.Panic(err)
	}
	client = cli_
}

func data() *table.Table {
	tbl, err := table.New("monitors_with_schema")
	if err != nil {
		log.Println(err)
	}

	// add column at first. This is to define the schema of the table.
	if err := tbl.AddTagColumn("id", types.INT64); err != nil {
		log.Println(err)
	}
	if err := tbl.AddFieldColumn("host", types.STRING); err != nil {
		log.Println(err)
	}
	if err := tbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		log.Println(err)
	}
	if err := tbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		log.Println(err)
	}

	if err := tbl.AddRow(1, "hello", 1.1, time.Now()); err != nil {
		log.Println(err)
	}
	if err := tbl.AddRow(2, "hello", 2.2, time.Now()); err != nil {
		log.Println(err)
	}

	return tbl
}

func write() {
	resp, err := client.Write(context.Background(), data())
	if err != nil {
		log.Println(err)
	}
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
}

func streamWrite() {
	ctx := context.Background()
	if err := client.StreamWrite(ctx, data()); err != nil {
		log.Println(err)
	}
	affected, err := client.CloseStream(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("affected rows: %d\n", affected.GetValue())
}

func main() {
	write()
	time.Sleep(time.Millisecond * 100)
	streamWrite()
}
