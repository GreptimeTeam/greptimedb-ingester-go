/*
 *    Copyright 2024 Greptime Team
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

const (
	// The GreptimeDB address.
	host = "127.0.0.1"

	// The database name.
	database = "public"
)

type Monitor struct {
	ID       int64     `greptime:"tag;column:id;type:int64"`
	JsonData string    `greptime:"column:my_json;type:json"`
	Ts       time.Time `greptime:"timestamp;column:ts;type:timestamp;precision:microsecond"`
}

type Person struct {
	Name      string
	Age       int
	IsStudent bool
	Courses   []string
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

func main() {
	c, err := newClient()
	if err != nil {
		log.Fatalf("failed to new client: %v", err)
	}

	tb, obj1, obj2, err := initData()
	if err != nil {
		log.Fatalf("failed to init data: %v", err)
	}

	if err = c.write(tb); err != nil {
		log.Fatalf("failed to write data: %v", err)
	}
	if err = c.writeObject(obj1); err != nil {
		log.Fatalf("failed to write object data: %v", err)
	}
	if err = c.streamWriteObject(obj2); err != nil {
		log.Fatalf("failed to stream write object data: %v", err)
	}
}

func (Monitor) TableName() string {
	return "json_data"
}

func initData() (*table.Table, *Monitor, *Monitor, error) {
	time1 := time.Now()
	time2 := time.Now()
	time3 := time.Now()

	tb, err := table.New("json_data")
	if err != nil {
		return nil, nil, nil, err
	}

	doeProfile := Person{
		Name:      "doe",
		Age:       25,
		IsStudent: false,
		Courses:   []string{"math", "history", "chemistry"},
	}

	// add column at first. This is to define the schema of the table.
	if err := tb.AddTagColumn("id", types.INT64); err != nil {
		return nil, nil, nil, err
	}
	if err := tb.AddFieldColumn("my_json", types.JSON); err != nil {
		return nil, nil, nil, err
	}
	if err := tb.AddTimestampColumn("ts", types.TIMESTAMP_MICROSECOND); err != nil {
		return nil, nil, nil, err
	}
	if err := tb.AddRow(1, doeProfile, time1); err != nil {
		return nil, nil, nil, err
	}

	weatherInfo := `{"city":"New York","temperature":22,"description":"Partly cloudy"}`
	weatherObj := &Monitor{
		ID:       2,
		JsonData: weatherInfo,
		Ts:       time2,
	}

	cherryProfile := Person{
		Name:      "Cherry",
		Age:       23,
		IsStudent: true,
		Courses:   []string{"archaeology", "physics"},
	}
	jsonData, err := json.Marshal(cherryProfile)
	if err != nil {
		return nil, nil, nil, err
	}
	cherryObj := &Monitor{
		ID:       3,
		JsonData: string(jsonData),
		Ts:       time3,
	}

	return tb, weatherObj, cherryObj, nil
}

func (c *client) write(data *table.Table) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := c.client.Write(ctx, data)
	if err != nil {
		return err
	}

	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
	return nil
}

func (c *client) writeObject(obj *Monitor) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	resp, err := c.client.WriteObject(ctx, obj)
	if err != nil {
		return err
	}

	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
	return nil
}

func (c *client) streamWriteObject(obj *Monitor) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := c.client.StreamWriteObject(ctx, obj); err != nil {
		return err
	}
	affected, err := c.client.CloseStream(ctx)
	if err != nil {
		return err
	}

	log.Printf("affected rows: %d\n", affected.GetValue())
	return nil
}
