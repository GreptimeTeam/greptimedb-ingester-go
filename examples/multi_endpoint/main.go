/*
 * Copyright 2026 Greptime Team
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

// Package main demonstrates writing to multiple GreptimeDB endpoints with
// client-side load balancing.
//
// Usage:
//
//	GREPTIMEDB_ENDPOINTS=host1:4001,host2:4001 go run ./examples/multi_endpoint -lb=rr
//
// The -lb flag selects the load-balancing strategy: "random" (default) or
// "rr" (round-robin). The example reports the observed dispatch counts per
// endpoint by wrapping the Picker.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/loadbalancer"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

const (
	database  = "public"
	tableName = "multi_endpoint_demo"
)

// countingPicker wraps a Picker and tallies how many times each endpoint
// gets selected, so the example can print a visible distribution at the end.
type countingPicker struct {
	inner  loadbalancer.Picker
	mu     sync.Mutex
	counts map[string]*atomic.Uint64
}

func newCountingPicker(inner loadbalancer.Picker) *countingPicker {
	return &countingPicker{inner: inner, counts: map[string]*atomic.Uint64{}}
}

func (p *countingPicker) Pick(endpoints []string) string {
	addr := p.inner.Pick(endpoints)
	p.mu.Lock()
	c, ok := p.counts[addr]
	if !ok {
		c = &atomic.Uint64{}
		p.counts[addr] = c
	}
	p.mu.Unlock()
	c.Add(1)
	return addr
}

func (p *countingPicker) Report() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	parts := make([]string, 0, len(p.counts))
	for addr, c := range p.counts {
		parts = append(parts, fmt.Sprintf("%s=%d", addr, c.Load()))
	}
	return strings.Join(parts, " ")
}

func pickerFromFlag(name string) loadbalancer.Picker {
	switch name {
	case "rr", "round-robin", "roundrobin":
		return loadbalancer.NewRoundRobin()
	case "random", "":
		return loadbalancer.NewRandom()
	default:
		log.Fatalf("unknown -lb value %q (want random|rr)", name)
		return nil
	}
}

func main() {
	lbName := flag.String("lb", "random", "load-balancer: random | rr")
	n := flag.Int("n", 20, "number of rows to write")
	flag.Parse()

	raw := os.Getenv("GREPTIMEDB_ENDPOINTS")
	if raw == "" {
		log.Fatal("set GREPTIMEDB_ENDPOINTS=host1:4001,host2:4001,...")
	}
	endpoints := strings.Split(raw, ",")
	for i := range endpoints {
		endpoints[i] = strings.TrimSpace(endpoints[i])
	}

	picker := newCountingPicker(pickerFromFlag(*lbName))

	cfg := greptime.NewConfig().
		WithDatabase(database).
		WithEndpoints(endpoints...).
		WithLoadBalancer(picker)

	client, err := greptime.NewClient(cfg)
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer func() { _ = client.Close() }()

	tbl, err := table.New(tableName)
	if err != nil {
		log.Fatalf("new table: %v", err)
	}
	if err := tbl.AddTagColumn("host", types.STRING); err != nil {
		log.Fatal(err)
	}
	if err := tbl.AddFieldColumn("cpu", types.FLOAT64); err != nil {
		log.Fatal(err)
	}
	if err := tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND); err != nil {
		log.Fatal(err)
	}
	if err := tbl.AddRow("127.0.0.1", 1.0, time.Now()); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < *n; i++ {
		resp, err := client.Write(ctx, tbl)
		if err != nil {
			log.Fatalf("write %d: %v", i, err)
		}
		_ = resp
	}

	log.Printf("lb=%s total=%d dispatch: %s", *lbName, *n, picker.Report())
}
