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
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

const (
	metricsBindAddr = ":2233"

	// It connects the OpenTelemetry Collector through local gRPC connection.
	// You may replace `localhost:4317` with your endpoint.
	tracingEndpoint = "localhost:4317"

	// The GreptimeDB address.
	host = "127.0.0.1"

	// The database name.
	database = "public"
)

type client struct {
	client *greptime.Client
}

func newClient(tracerProvider trace.TracerProvider, meterProvider *metric.MeterProvider) (*client, error) {
	cfg := greptime.NewConfig(host).WithDatabase(database).
		WithTraceProvider(tracerProvider).WithTracesEnabled(true).
		WithMeterProvider(meterProvider).WithMetricsEnabled(true)

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
	log.Printf("Waiting for connection...")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	conn, err := initConn()
	if err != nil {
		log.Fatal(err)
	}

	serviceName := semconv.ServiceNameKey.String("test-otel")
	res, err := resource.New(ctx,
		resource.WithAttributes(
			// The service name used to display traces in backends
			serviceName,
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	tracerProvider, err := initTracerProvider(ctx, res, conn)
	if err != nil {
		log.Fatal(err)
	}

	exporter, err := otelprom.New(otelprom.WithNamespace("greptime"))
	if err != nil {
		log.Fatal(err)
	}
	meterProvider := metric.NewMeterProvider(
		metric.WithReader(exporter),
	)

	if err = initMeterProvider(ctx, res, conn); err != nil {
		log.Fatal(err)
	}

	// Start the prometheus HTTP server and pass the exporter Collector to it
	go serveMetrics()

	c, err := newClient(tracerProvider, meterProvider)
	if err != nil {
		log.Fatalf("failed to new client: %v:", err)
	}

	data, err := initData()
	if err != nil {
		log.Fatalf("failed to init data: %v:", err)
	}
	if err = c.write(data[1]); err != nil {
		log.Fatalf("failed to write data: %v:", err)
	}

	log.Printf("Sleep 30s...")
	time.Sleep(30 * time.Second)
	log.Printf("Done!")
}

// Initialize a gRPC connection to be used by both the tracer and meter
// providers.
func initConn() (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(tracingEndpoint,
		// Note the use of insecure transport here. TLS is recommended in production.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	return conn, err
}

// Initializes an OTLP exporter, and configures the corresponding trace provider.
func initTracerProvider(ctx context.Context, res *resource.Resource, conn *grpc.ClientConn) (trace.TracerProvider, error) {
	// Set up a trace exporter
	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// Register the trace exporter with a TracerProvider, using a batch
	// span processor to aggregate spans before export.
	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)
	otel.SetTracerProvider(tracerProvider)

	// Set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Shutdown will flush any remaining spans and shut down the exporter.
	return tracerProvider, nil
}

// Initializes an OTLP exporter, and configures the corresponding meter provider.
func initMeterProvider(ctx context.Context, res *resource.Resource, conn *grpc.ClientConn) error {
	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return fmt.Errorf("failed to create metrics exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

	return nil
}

func serveMetrics() {
	log.Printf("serving metrics at localhost%s/metrics", metricsBindAddr)
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(metricsBindAddr, nil) //nolint:gosec // Ignoring G114: Use of net/http serve function that has no support for setting timeouts.
	if err != nil {
		log.Fatalf("error serving http: %v", err)
	}
}

func initData() ([]*table.Table, error) {
	time1 := time.Now()
	time2 := time.Now()
	time3 := time.Now()

	itbl, err := table.New("monitors_with_schema")
	if err != nil {
		return nil, err
	}
	// add column at first. This is to define the schema of the table.
	if err := itbl.AddTagColumn("id", types.INT64); err != nil {
		return nil, err
	}
	if err := itbl.AddFieldColumn("host", types.STRING); err != nil {
		return nil, err
	}
	if err := itbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		return nil, err
	}
	if err := itbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		return nil, err
	}

	if err := itbl.AddRow(1, "hello", 1.1, time1); err != nil {
		return nil, err
	}
	if err := itbl.AddRow(2, "hello", 2.2, time2); err != nil {
		return nil, err
	}
	if err := itbl.AddRow(3, "hello", 3.3, time3); err != nil {
		return nil, err
	}

	utbl, err := table.New("monitors_with_schema")
	if err != nil {
		return nil, err
	}

	// add column at first. This is to define the schema of the table.
	if err := utbl.AddTagColumn("id", types.INT64); err != nil {
		return nil, err
	}
	if err := utbl.AddFieldColumn("host", types.STRING); err != nil {
		return nil, err
	}
	if err := utbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		return nil, err
	}
	if err := utbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		return nil, err
	}

	if err := utbl.AddRow(1, "hello", 1.2, time1); err != nil {
		return nil, err
	}

	dtbl, err := table.New("monitors_with_schema")
	if err != nil {
		return nil, err
	}

	// add column at first. This is to define the schema of the table.
	if err := dtbl.AddTagColumn("id", types.INT64); err != nil {
		return nil, err
	}
	if err := dtbl.AddFieldColumn("host", types.STRING); err != nil {
		return nil, err
	}
	if err := dtbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		return nil, err
	}
	if err := dtbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		return nil, err
	}

	if err := dtbl.AddRow(3, "hello", 3.3, time3); err != nil {
		return nil, err
	}

	return []*table.Table{itbl, utbl, dtbl}, nil
}

func (c client) write(data *table.Table) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := c.client.Write(ctx, data)
	if err != nil {
		return err
	}
	log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
	return nil
}
