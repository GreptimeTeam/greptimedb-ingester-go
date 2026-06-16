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
	"time"
)

// Selector picks one endpoint per call, honoring an exclude set so a retry loop
// can steer away from peers already tried and failed in the current sequence.
// Exclusion is best-effort: a selector must fall open to the full set rather
// than fail when every endpoint is excluded, because a retry against the
// least-bad peer beats no retry at all.
//
// A Picker that also implements Selector is used directly by the client;
// otherwise AsSelector wraps it. NewOutlierDetector implements both Picker (so
// it can be passed to Config.WithLoadBalancer) and Selector.
type Selector interface {
	Select(endpoints []string, exclude map[string]struct{}) string
}

// HealthReporter is an optional interface a Picker may implement to learn
// endpoint health from call outcomes. The client invokes ReportFailure on
// transport-level endpoint failures and ReportSuccess when an endpoint answers
// (including server business errors — the endpoint is alive and routing
// correctly). A healthy frontend must never be ejected for a datanode-side
// condition.
type HealthReporter interface {
	ReportSuccess(endpoint string)
	ReportFailure(endpoint string)
}

// applyExclude drops excluded peers, falling open to the full list when that
// would leave nothing to choose from.
func applyExclude(endpoints []string, exclude map[string]struct{}) []string {
	if len(exclude) == 0 {
		return endpoints
	}
	kept := make([]string, 0, len(endpoints))
	for _, ep := range endpoints {
		if _, skip := exclude[ep]; !skip {
			kept = append(kept, ep)
		}
	}
	if len(kept) == 0 {
		return endpoints
	}
	return kept
}

// AsSelector adapts a Picker into a Selector. A Picker that already implements
// Selector is returned unchanged; otherwise it is wrapped so exclude is honored
// by pre-filtering the candidate list before delegating to Pick.
func AsSelector(p Picker) Selector {
	if s, ok := p.(Selector); ok {
		return s
	}
	return pickerSelector{p}
}

type pickerSelector struct{ Picker }

func (s pickerSelector) Select(endpoints []string, exclude map[string]struct{}) string {
	return s.Pick(applyExclude(endpoints, exclude))
}

// OutlierDetectorOptions configures NewOutlierDetector. Zero-value fields take
// the documented defaults.
type OutlierDetectorOptions struct {
	// Base chooses among the currently-healthy peers. Defaults to NewRandom().
	Base Picker
	// ConsecutiveFailures is the number of back-to-back endpoint failures
	// before an endpoint is ejected from selection. Defaults to 5.
	ConsecutiveFailures int
	// BaseEjection is the first ejection duration; it doubles on each
	// re-ejection up to MaxEjection. Defaults to 30s.
	BaseEjection time.Duration
	// MaxEjection caps a single ejection duration. Defaults to 300s.
	MaxEjection time.Duration

	// now is an injectable clock for tests. Defaults to time.Now.
	now func() time.Time
}

type endpointHealth struct {
	consecutiveFailures int
	ejectedUntil        time.Time
	ejectionCount       int
}

// OutlierDetector wraps a base Picker and removes endpoints that fail
// ConsecutiveFailures times in a row from the candidate set for a back-off
// window. Health is fed by ReportSuccess / ReportFailure from call outcomes.
// Ejection is time-based and re-admission is lazy (checked against the clock at
// Select time, no background timers). If every endpoint is ejected it falls
// open to the full set so writes never hard-stall on a transient cluster-wide
// failure. Safe for concurrent use.
//
// A stateful detector instance keys health by endpoint address and must not be
// shared across clients configured with different endpoint sets.
type OutlierDetector struct {
	base         Picker
	threshold    int
	baseEjection time.Duration
	maxEjection  time.Duration
	now          func() time.Time

	mu     sync.Mutex
	health map[string]*endpointHealth
}

// NewOutlierDetector returns a health-aware Selector with Envoy-style
// consecutive-failure outlier ejection.
func NewOutlierDetector(opts OutlierDetectorOptions) *OutlierDetector {
	base := opts.Base
	if base == nil {
		base = NewRandom()
	}
	threshold := opts.ConsecutiveFailures
	if threshold < 1 {
		threshold = 5
	}
	baseEjection := opts.BaseEjection
	if baseEjection <= 0 {
		baseEjection = 30 * time.Second
	}
	maxEjection := opts.MaxEjection
	if maxEjection <= 0 {
		maxEjection = 300 * time.Second
	}
	now := opts.now
	if now == nil {
		now = time.Now
	}
	return &OutlierDetector{
		base:         base,
		threshold:    threshold,
		baseEjection: baseEjection,
		maxEjection:  maxEjection,
		now:          now,
		health:       make(map[string]*endpointHealth),
	}
}

// Pick selects among the currently-healthy endpoints without exclusion.
func (o *OutlierDetector) Pick(endpoints []string) string {
	return o.Select(endpoints, nil)
}

// Select returns a healthy, non-excluded endpoint chosen by the base picker.
func (o *OutlierDetector) Select(endpoints []string, exclude map[string]struct{}) string {
	now := o.now()

	o.mu.Lock()
	healthy := make([]string, 0, len(endpoints))
	for _, ep := range endpoints {
		h := o.health[ep]
		if h == nil || !h.ejectedUntil.After(now) {
			healthy = append(healthy, ep)
		}
	}
	o.mu.Unlock()

	candidates := healthy
	if len(candidates) == 0 {
		candidates = endpoints
	}
	return o.base.Pick(applyExclude(candidates, exclude))
}

// ReportSuccess clears any failure streak for endpoint and re-admits it.
func (o *OutlierDetector) ReportSuccess(endpoint string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if h := o.health[endpoint]; h != nil {
		// A fresh success outweighs stale failures: clear the streak and
		// re-admit immediately. During an active ejection the endpoint is not
		// selected, so a success can only arrive via a fall-open window —
		// exactly when we want to re-admit the one peer that works.
		h.consecutiveFailures = 0
		h.ejectedUntil = time.Time{}
	}
}

// ReportFailure records a transport-level failure for endpoint and ejects it
// once the consecutive-failure threshold is crossed.
func (o *OutlierDetector) ReportFailure(endpoint string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	h := o.health[endpoint]
	if h == nil {
		h = &endpointHealth{}
		o.health[endpoint] = h
	}
	h.consecutiveFailures++

	now := o.now()
	// Only (re)eject when not already ejected, so failures observed during a
	// fall-open window don't compound the back-off prematurely.
	if h.consecutiveFailures >= o.threshold && !h.ejectedUntil.After(now) {
		// Window doubles per prior ejection, capped at maxEjection. ejectionCount
		// never resets, so guard the shift against int64 overflow on a long-lived
		// flapping endpoint: a too-large or wrapped value falls back to the cap.
		duration := o.maxEjection
		if h.ejectionCount < 63 {
			if scaled := o.baseEjection << h.ejectionCount; scaled > 0 && scaled < o.maxEjection {
				duration = scaled
			}
		}
		h.ejectedUntil = now.Add(duration)
		h.ejectionCount++
		h.consecutiveFailures = 0
	}
}
