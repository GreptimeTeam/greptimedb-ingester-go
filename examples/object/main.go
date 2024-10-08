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

type Monitor struct {
	ID          int64     `greptime:"tag;column:id;type:int64"`
	Host        string    `greptime:"tag;column:host;type:string"`
	Memory      uint64    `greptime:"field;column:memory;type:uint64"`
	Cpu         float64   `greptime:"field;column:cpu;type:float64"`
	Temperature int64     `greptime:"field;column:temperature;type:int64"`
	Running     bool      `greptime:"field;column:running;type:boolean"`
	Ts          time.Time `greptime:"timestamp;column:ts;type:timestamp;precision:millisecond"`
}

func (Monitor) TableName() string {
	return "monitors_with_tag"
}

func initData() []Monitor {
	return []Monitor{
		{
			Host:        "127.0.0.1",
			Memory:      1,
			Cpu:         1.3,
			Temperature: -1,
			Ts:          time.Now(),
		},
		{
			ID:          1,
			Host:        "127.0.0.2",
			Memory:      1,
			Cpu:         1.0,
			Temperature: -1,
			Ts:          time.Now(),
			Running:     true,
		},
		{
			ID:          2,
			Host:        "127.0.0.3",
			Memory:      2,
			Cpu:         2.0,
			Temperature: -2,
			Ts:          time.Now(),
			Running:     true,
		},
		{
			ID:          3,
			Host:        "127.0.0.4",
			Memory:      3,
			Cpu:         3.0,
			Temperature: -3,
			Ts:          time.Now(),
			Running:     true,
		},
	}
}

func writeObject(data []Monitor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	resp, err := client.WriteObject(ctx, data)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
}

func deleteObject(data []Monitor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	resp, err := client.DeleteObject(ctx, data)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
}

func streamWriteObject(data []Monitor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := client.StreamWriteObject(ctx, data); err != nil {
		log.Println(err)
	}
	affected, err := client.CloseStream(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("affected rows: %d\n", affected.GetValue())
}

func streamDeleteObject(data []Monitor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := client.StreamDeleteObject(ctx, data); err != nil {
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
	writeObject(data)
	// update
	data[1].Cpu = 1.1
	writeObject(data)
	// delete
	deleteObject(data[3:])

	time.Sleep(time.Millisecond * 100)

	data = initData()
	// stream insert
	streamWriteObject(data)
	data[1].Cpu = 1.1
	// stream update
	streamWriteObject(data)
	// stream delete
	streamDeleteObject(data[3:])
}
