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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyExcludeFallsOpenWhenAllExcluded(t *testing.T) {
	eps := []string{"a", "b"}
	assert.Equal(t, eps, applyExclude(eps, nil))
	assert.Equal(t, []string{"b"}, applyExclude(eps, map[string]struct{}{"a": {}}))
	// Everything excluded → fall open to the full list.
	assert.Equal(t, eps, applyExclude(eps, map[string]struct{}{"a": {}, "b": {}}))
}

func TestAsSelectorWrapsPlainPicker(t *testing.T) {
	// RoundRobin is a plain Picker; the adapter must honor exclude.
	s := AsSelector(NewRoundRobin())
	got := s.Select([]string{"a", "b"}, map[string]struct{}{"a": {}})
	assert.Equal(t, "b", got)
}

func TestAsSelectorPassesThroughExistingSelector(t *testing.T) {
	od := NewOutlierDetector(OutlierDetectorOptions{})
	assert.Same(t, od, AsSelector(od))
}

// fixedClock is a controllable monotonic clock for deterministic ejection tests.
type fixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fixedClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func newTestDetector(clk *fixedClock, opts OutlierDetectorOptions) *OutlierDetector {
	opts.now = clk.Now
	return NewOutlierDetector(opts)
}

func TestOutlierDetectorEjectsAfterConsecutiveFailures(t *testing.T) {
	clk := &fixedClock{now: time.Unix(0, 0)}
	od := newTestDetector(clk, OutlierDetectorOptions{
		Base:                NewRoundRobin(),
		ConsecutiveFailures: 3,
		BaseEjection:        30 * time.Second,
	})
	eps := []string{"a", "b"}

	// Two failures: below threshold, still selectable.
	od.ReportFailure("a")
	od.ReportFailure("a")
	assert.Contains(t, candidatesOver(od, eps, 20), "a")

	// Third failure crosses the threshold → "a" is ejected.
	od.ReportFailure("a")
	picks := candidatesOver(od, eps, 20)
	assert.NotContains(t, picks, "a", "ejected endpoint must not be selected")
	assert.Contains(t, picks, "b")
}

func TestOutlierDetectorReadmitsAfterEjectionWindow(t *testing.T) {
	clk := &fixedClock{now: time.Unix(0, 0)}
	od := newTestDetector(clk, OutlierDetectorOptions{
		ConsecutiveFailures: 1,
		BaseEjection:        30 * time.Second,
	})
	eps := []string{"a", "b"}

	od.ReportFailure("a")
	assert.NotContains(t, candidatesOver(od, eps, 20), "a")

	// Before the window expires, still ejected.
	clk.advance(29 * time.Second)
	assert.NotContains(t, candidatesOver(od, eps, 20), "a")

	// After the window, re-admitted (lazy, no timers).
	clk.advance(2 * time.Second)
	assert.Contains(t, candidatesOver(od, eps, 20), "a")
}

func TestOutlierDetectorBackoffDoubles(t *testing.T) {
	clk := &fixedClock{now: time.Unix(0, 0)}
	od := newTestDetector(clk, OutlierDetectorOptions{
		ConsecutiveFailures: 1,
		BaseEjection:        10 * time.Second,
		MaxEjection:         60 * time.Second,
	})
	eps := []string{"a", "b"}

	// First ejection: 10s window.
	od.ReportFailure("a")
	clk.advance(11 * time.Second) // expires
	assert.Contains(t, candidatesOver(od, eps, 20), "a")

	// Second ejection should last 20s (doubled).
	od.ReportFailure("a")
	clk.advance(11 * time.Second)
	assert.NotContains(t, candidatesOver(od, eps, 20), "a", "second ejection must be longer than 10s")
	clk.advance(10 * time.Second) // now past 20s total
	assert.Contains(t, candidatesOver(od, eps, 20), "a")
}

func TestOutlierDetectorBackoffNeverWrapsOnManyEjections(t *testing.T) {
	clk := &fixedClock{now: time.Unix(0, 0)}
	od := newTestDetector(clk, OutlierDetectorOptions{
		ConsecutiveFailures: 1,
		BaseEjection:        30 * time.Second,
		MaxEjection:         300 * time.Second,
	})
	eps := []string{"a", "b"}

	// Re-eject "a" many times; ejectionCount climbs well past the int64 shift
	// width. Each window must stay within (0, maxEjection] — never wrap to a
	// short or negative window.
	for i := 0; i < 70; i++ {
		od.ReportFailure("a")
		od.mu.Lock()
		h := od.health["a"]
		window := h.ejectedUntil.Sub(clk.Now())
		od.mu.Unlock()
		assert.Greater(t, window, time.Duration(0), "ejection %d window must be positive", i)
		assert.LessOrEqual(t, window, 300*time.Second, "ejection %d window must not exceed maxEjection", i)
		// Expire the window so the next ReportFailure re-ejects.
		clk.advance(window + time.Second)
		_ = candidatesOver(od, eps, 1) // lazy re-admission check
	}
}

func TestOutlierDetectorSuccessResetsStreak(t *testing.T) {
	clk := &fixedClock{now: time.Unix(0, 0)}
	od := newTestDetector(clk, OutlierDetectorOptions{ConsecutiveFailures: 3})
	eps := []string{"a", "b"}

	od.ReportFailure("a")
	od.ReportFailure("a")
	od.ReportSuccess("a") // resets streak
	od.ReportFailure("a")
	od.ReportFailure("a")
	// Only two consecutive failures since the reset: not yet ejected.
	assert.Contains(t, candidatesOver(od, eps, 20), "a")
}

func TestOutlierDetectorFallsOpenWhenAllEjected(t *testing.T) {
	clk := &fixedClock{now: time.Unix(0, 0)}
	od := newTestDetector(clk, OutlierDetectorOptions{ConsecutiveFailures: 1})
	eps := []string{"a", "b"}

	od.ReportFailure("a")
	od.ReportFailure("b")
	// Both ejected → fall open so writes never hard-stall.
	got := od.Select(eps, nil)
	assert.Contains(t, eps, got)
}

func TestOutlierDetectorExcludeStillHonored(t *testing.T) {
	clk := &fixedClock{now: time.Unix(0, 0)}
	od := newTestDetector(clk, OutlierDetectorOptions{Base: NewRoundRobin(), ConsecutiveFailures: 5})
	// No ejections; exclude "a" should steer to "b".
	got := od.Select([]string{"a", "b"}, map[string]struct{}{"a": {}})
	assert.Equal(t, "b", got)
}

func TestNewOutlierDetectorDefaults(t *testing.T) {
	od := NewOutlierDetector(OutlierDetectorOptions{})
	assert.Equal(t, 5, od.threshold)
	assert.Equal(t, 30*time.Second, od.baseEjection)
	assert.Equal(t, 300*time.Second, od.maxEjection)
	require.NotNil(t, od.base)
	require.NotNil(t, od.now)
}

// candidatesOver samples Select n times and returns the set of distinct peers
// chosen, so a test can assert whether an ejected endpoint ever appears.
func candidatesOver(od *OutlierDetector, eps []string, n int) map[string]struct{} {
	seen := make(map[string]struct{})
	for i := 0; i < n; i++ {
		seen[od.Select(eps, nil)] = struct{}{}
	}
	return seen
}
