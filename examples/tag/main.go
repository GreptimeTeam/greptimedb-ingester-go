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

	"github.com/GreptimeTeam/greptimedb-ingester-go/client"
	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
)

var (
	cli    *client.Client
	stream *client.StreamClient
)

func init() {
	cfg := config.New("127.0.0.1").WithDatabase("public")

	cli_, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	cli = cli_

	stream_, err := client.NewStreamClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	stream = stream_
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

func main() {

	monitors := []Monitor{
		{
			ID:          1,
			Host:        "127.0.0.1",
			Memory:      1,
			Cpu:         1.0,
			Temperature: -1,
			Ts:          time.Now(),
			Running:     true,
		},
		{
			ID:          2,
			Host:        "127.0.0.2",
			Memory:      2,
			Cpu:         2.0,
			Temperature: -2,
			Ts:          time.Now(),
			Running:     true,
		},
	}

	{ // client write data into GreptimeDB
		_, err := cli.Create(context.Background(), monitors)
		if err != nil {
			log.Fatal(err)
		}
	}

	{ // stream client send data into GreptimeDB
		err := stream.Create(context.Background(), monitors)
		if err != nil {
			log.Fatal(err)
		}
	}

}
