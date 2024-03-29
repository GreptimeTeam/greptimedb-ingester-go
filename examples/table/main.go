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

	itbl, err := table.New("monitors_with_schema")
	if err != nil {
		log.Println(err)
	}
	// add column at first. This is to define the schema of the table.
	if err := itbl.AddTagColumn("id", types.INT64); err != nil {
		log.Println(err)
	}
	if err := itbl.AddFieldColumn("host", types.STRING); err != nil {
		log.Println(err)
	}
	if err := itbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		log.Println(err)
	}
	if err := itbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		log.Println(err)
	}

	if err := itbl.AddRow(1, "hello", 1.1, time1); err != nil {
		log.Println(err)
	}
	if err := itbl.AddRow(2, "hello", 2.2, time2); err != nil {
		log.Println(err)
	}
	if err := itbl.AddRow(3, "hello", 3.3, time3); err != nil {
		log.Println(err)
	}

	utbl, err := table.New("monitors_with_schema")
	if err != nil {
		log.Println(err)
	}

	// add column at first. This is to define the schema of the table.
	if err := utbl.AddTagColumn("id", types.INT64); err != nil {
		log.Println(err)
	}
	if err := utbl.AddFieldColumn("host", types.STRING); err != nil {
		log.Println(err)
	}
	if err := utbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		log.Println(err)
	}
	if err := utbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		log.Println(err)
	}

	if err := utbl.AddRow(1, "hello", 1.2, time1); err != nil {
		log.Println(err)
	}

	dtbl, err := table.New("monitors_with_schema")
	if err != nil {
		log.Println(err)
	}

	// add column at first. This is to define the schema of the table.
	if err := dtbl.AddTagColumn("id", types.INT64); err != nil {
		log.Println(err)
	}
	if err := dtbl.AddFieldColumn("host", types.STRING); err != nil {
		log.Println(err)
	}
	if err := dtbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		log.Println(err)
	}
	if err := dtbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		log.Println(err)
	}

	if err := dtbl.AddRow(3, "hello", 3.3, time3); err != nil {
		log.Println(err)
	}

	return []*table.Table{itbl, utbl, dtbl}
}

func write(data *table.Table) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := client.Write(ctx, data)
	if err != nil {
		log.Println(err)
	}
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
}

func delete(data *table.Table) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := client.Delete(ctx, data)
	if err != nil {
		log.Println(err)
	}
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
}

func streamWrite(data *table.Table) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	if err := client.StreamWrite(ctx, data); err != nil {
		log.Println(err)
	}
	affected, err := client.CloseStream(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("affected rows: %d\n", affected.GetValue())
}

func streamDelete(data *table.Table) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	if err := client.StreamDelete(ctx, data); err != nil {
		log.Println(err)
	}
	affected, err := client.CloseStream(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("affected rows: %d\n", affected.GetValue())
}

func main() {
	data := initData()
	// insert
	write(data[INSERT])
	// update
	write(data[UPDATE])
	// delete
	delete(data[DELETE])

	time.Sleep(time.Millisecond * 100)

	data = initData()
	// stream insert
	streamWrite(data[INSERT])
	// stream update
	streamWrite(data[UPDATE])
	// stream delete
	streamDelete(data[DELETE])

}
