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

package loadbalancer

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomPickerCoversAllEndpoints(t *testing.T) {
	picker := NewRandom()
	endpoints := []string{"a:1", "b:1", "c:1"}
	seen := map[string]int{}
	const iters = 3000
	for i := 0; i < iters; i++ {
		seen[picker.Pick(endpoints)]++
	}

	assert.Len(t, seen, len(endpoints), "all endpoints should be picked at least once")
	for _, addr := range endpoints {
		// Expected ~iters/len; tolerate wide skew so the test is not flaky.
		assert.Greater(t, seen[addr], iters/10,
			"endpoint %s picked %d times, expected significant share", addr, seen[addr])
	}
}

func TestRoundRobinPickerRotates(t *testing.T) {
	picker := NewRoundRobin()
	endpoints := []string{"a:1", "b:1", "c:1"}
	want := []string{"a:1", "b:1", "c:1", "a:1", "b:1", "c:1", "a:1"}
	for i, exp := range want {
		got := picker.Pick(endpoints)
		assert.Equal(t, exp, got, "iteration %d", i)
	}
}

func TestRoundRobinPickerConcurrentSafe(t *testing.T) {
	picker := NewRoundRobin()
	endpoints := []string{"a:1", "b:1", "c:1", "d:1"}

	const goroutines = 16
	const perG = 500
	counts := make(map[string]*atomic.Uint64, len(endpoints))
	for _, ep := range endpoints {
		counts[ep] = &atomic.Uint64{}
	}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				counts[picker.Pick(endpoints)].Add(1)
			}
		}()
	}
	wg.Wait()

	total := uint64(0)
	for _, c := range counts {
		total += c.Load()
	}
	assert.Equal(t, uint64(goroutines*perG), total)

	// Perfect rotation distributes total/len to each endpoint. The atomic
	// counter guarantees exact fairness regardless of goroutine interleaving.
	expected := total / uint64(len(endpoints))
	for ep, c := range counts {
		assert.Equal(t, expected, c.Load(),
			"endpoint %s count %d, expected %d", ep, c.Load(), expected)
	}
}
