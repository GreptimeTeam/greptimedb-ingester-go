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
	"log"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
)

const (
	// The GreptimeDB address.
	host = "127.0.0.1"

	// The database name.
	database = "public"
)

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

	_, err = c.client.HealthCheck(context.Background())
	if err != nil {
		log.Println("failed to health check:", err)
		return
	}
	log.Println("the greptimedb is health")
}
