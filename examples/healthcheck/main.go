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

func main() {
	_, err := client.HealthCheck(context.Background())
	if err != nil {
		log.Println("failed to health check:", err)
		return
	}
	log.Println("the greptimedb is health")
}
