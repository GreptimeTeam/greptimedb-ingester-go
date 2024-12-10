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

const (
	// The GreptimeDB address.
	host = "127.0.0.1"

	// The database name.
	database = "public"
)

type Monitor struct {
	ID          int64     `greptime:"tag;column:id;type:int64"`
	Host        string    `greptime:"tag;column:host;type:string"`
	Memory      uint64    `greptime:"-"`
	Cpu         float64   `greptime:"field;column:cpu;type:float64"`
	Temperature int64     `greptime:"-"`
	Running     bool      `greptime:"field;column:running;type:boolean"`
	Ts          time.Time `greptime:"timestamp;column:ts;type:timestamp;precision:millisecond"`
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

func (Monitor) TableName() string {
	return "monitors_with_ignore_field"
}

func initData() []Monitor {
	return []Monitor{
		{
			ID:          0,
			Host:        "127.0.0.1",
			Memory:      1,
			Ts:          time.Now(),
			Cpu:         1.3,
			Temperature: -1,
			Running:     false,
		},
		{
			ID:          1,
			Host:        "127.0.0.2",
			Memory:      2,
			Ts:          time.Now(),
			Cpu:         3.2,
			Temperature: 1,
			Running:     true,
		},
	}
}

func (c *client) writeObject(data []Monitor) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	resp, err := c.client.WriteObject(ctx, data)
	if err != nil {
		return err
	}

	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
	return nil
}

func (c *client) streamWriteObject(data []Monitor) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := c.client.StreamWriteObject(ctx, data); err != nil {
		return err
	}
	affected, err := c.client.CloseStream(ctx)
	if err != nil {
		return err
	}

	log.Printf("affected rows: %d\n", affected.GetValue())
	return nil
}

func main() {
	c, err := newClient()
	if err != nil {
		log.Fatalf("failed to new client: %v:", err)
	}

	data := initData()
	if err = c.writeObject(data[:1]); err != nil {
		log.Fatalf("failed to write data: %v:", err)
	}

	if err = c.streamWriteObject(data[1:]); err != nil {
		log.Fatalf("failed to stream write data: %v:", err)
	}
}
