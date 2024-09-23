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

	"github.com/GreptimeTeam/greptimedb-ingester-go/pkg/hint"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

const (
	INSERT = 0
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

func initData() []*table.Table {
	time1 := time.Now()
	time2 := time.Now()
	time3 := time.Now()

	itbl, err := table.New("monitor_table_with_hints")
	if err != nil {
		log.Println(err)
		return nil
	}
	// add column at first. This is to define the schema of the table.
	if err := itbl.AddTagColumn("id", types.INT64); err != nil {
		log.Println(err)
		return nil
	}
	if err := itbl.AddFieldColumn("host", types.STRING); err != nil {
		log.Println(err)
		return nil
	}
	if err := itbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		log.Println(err)
		return nil
	}
	if err := itbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		log.Println(err)
		return nil
	}

	if err := itbl.AddRow(1, "hello", 1.1, time1); err != nil {
		log.Println(err)
		return nil
	}
	if err := itbl.AddRow(2, "hello", 2.2, time2); err != nil {
		log.Println(err)
		return nil
	}
	if err := itbl.AddRow(3, "hello", 3.3, time3); err != nil {
		log.Println(err)
		return nil
	}

	return []*table.Table{itbl}
}

func write(data *table.Table) {
	var hints []hint.Hint
	hints = append(hints,
		hint.Hint{Key: "ttl", Value: "3d"},
		hint.Hint{Key: "merge_mode", Value: "last_non_null"},
		hint.Hint{Key: "append_mode", Value: "false"},
	)

	ctx := hint.CreateContextWithHints(hints)
	ctx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()

	resp, err := client.Write(ctx, data)
	if err != nil {
		log.Println(err)
		return
	}
	tableName, err := data.GetName()
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("create table, name: '%s'", tableName)
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
}

func main() {
	data := initData()

	write(data[INSERT])
}
