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

// Package loadbalancer provides pluggable strategies for distributing
// requests across multiple GreptimeDB endpoints.
package loadbalancer

import (
	"math/rand/v2"
	"sync/atomic"
)

// Picker selects one endpoint from the given slice per call.
// Implementations must be safe for concurrent use. Callers must pass a
// non-empty slice; behavior on an empty slice is undefined and the
// built-in pickers will panic.
type Picker interface {
	Pick(endpoints []string) string
}

type randomPicker struct{}

// NewRandom returns a stateless Picker that uniformly picks an endpoint at
// random on every call. This is the default strategy.
func NewRandom() Picker {
	return randomPicker{}
}

func (randomPicker) Pick(endpoints []string) string {
	return endpoints[rand.IntN(len(endpoints))]
}

type roundRobinPicker struct {
	counter atomic.Uint64
}

// NewRoundRobin returns a Picker that rotates through endpoints in order.
// Safe for concurrent use via an atomic counter.
func NewRoundRobin() Picker {
	return &roundRobinPicker{}
}

func (p *roundRobinPicker) Pick(endpoints []string) string {
	n := p.counter.Add(1) - 1
	return endpoints[n%uint64(len(endpoints))]
}
