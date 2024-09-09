// Copyright 2024 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package options

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
)

// TelemetryOptions defines the configurable settings for SDK telemetry.
type TelemetryOptions struct {
	Metrics MetricsOptions
	Traces  TracesOptions
}

// MetricsOptions defines the configuration for SDK's metrics.
type MetricsOptions struct {
	Enabled       bool
	MeterProvider metric.MeterProvider
}

// TracesOptions exposes the configuration for SDK's traces.
type TracesOptions struct {
	Enabled        bool
	TracerProvider trace.TracerProvider
}

// NewTelemetryOptions returns a TelemetryOptions with default settings.
func NewTelemetryOptions() *TelemetryOptions {
	return &TelemetryOptions{
		Metrics: MetricsOptions{
			Enabled:       false,
			MeterProvider: nil,
		},
		Traces: TracesOptions{
			Enabled:        false,
			TracerProvider: nil,
		},
	}
}

// Build returns a grpc.DialOption to configure grpc client telemetry.
func (o *TelemetryOptions) Build() grpc.DialOption {
	if !o.Metrics.Enabled && !o.Traces.Enabled {
		return grpc.EmptyDialOption{}
	}

	// otelgrpc will use the global meter/tracer provider by default
	// set providers to the noop to disable signals collection
	if !o.Metrics.Enabled {
		o.Metrics.MeterProvider = metricnoop.NewMeterProvider()
	}
	if !o.Traces.Enabled {
		o.Traces.TracerProvider = tracenoop.NewTracerProvider()
	}

	return grpc.WithStatsHandler(otelgrpc.NewClientHandler(
		otelgrpc.WithMeterProvider(o.Metrics.MeterProvider),
		otelgrpc.WithTracerProvider(o.Traces.TracerProvider),
	))
}
