/*
 * Copyright 2024 Greptime Team
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
	Memory      uint64    `greptime:"field;column:memory;type:uint64"`
	Cpu         float64   `greptime:"field;column:cpu;type:float64"`
	Temperature int64     `greptime:"field;column:temperature;type:int64"`
	Running     bool      `greptime:"field;column:running;type:boolean"`
	Ts          time.Time `greptime:"timestamp;column:ts;type:timestamp;precision:millisecond"`
	DTT         time.Time `greptime:"field;column:dtt;type:datetime"`
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
			DTT:         time.Now(),
		},
		{
			ID:          1,
			Host:        "127.0.0.2",
			Memory:      1,
			Cpu:         1.0,
			Temperature: -1,
			Ts:          time.Now(),
			Running:     true,
			DTT:         time.Now(),
		},
		{
			ID:          2,
			Host:        "127.0.0.3",
			Memory:      2,
			Cpu:         2.0,
			Temperature: -2,
			Ts:          time.Now(),
			Running:     true,
			DTT:         time.Now(),
		},
		{
			ID:          3,
			Host:        "127.0.0.4",
			Memory:      3,
			Cpu:         3.0,
			Temperature: -3,
			Ts:          time.Now(),
			Running:     true,
			DTT:         time.Now(),
		},
	}
}

func (c *client) writeObject(data []Monitor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	resp, err := c.client.WriteObject(ctx, data)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
}

func (c *client) deleteObject(data []Monitor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	resp, err := c.client.DeleteObject(ctx, data)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
}

func (c *client) streamWriteObject(data []Monitor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := c.client.StreamWriteObject(ctx, data); err != nil {
		log.Fatal(err)
	}
	affected, err := c.client.CloseStream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("affected rows: %d\n", affected.GetValue())
}

func (c *client) streamDeleteObject(data []Monitor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := c.client.StreamDeleteObject(ctx, data); err != nil {
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
	defer c.client.Close()

	data := initData()
	// insert
	c.writeObject(data)
	// update
	data[1].Cpu = 1.1
	c.writeObject(data)
	// delete
	c.deleteObject(data[3:])

	time.Sleep(time.Millisecond * 100)

	data = initData()
	// stream insert
	c.streamWriteObject(data)
	data[1].Cpu = 1.1
	// stream update
	c.streamWriteObject(data)
	// stream delete
	c.streamDeleteObject(data[3:])
}
