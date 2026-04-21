/*
 * Copyright 2023 Greptime Team
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

package greptime

import (
	"fmt"
	"net"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"google.golang.org/grpc"

	"github.com/GreptimeTeam/greptimedb-ingester-go/loadbalancer"
	"github.com/GreptimeTeam/greptimedb-ingester-go/options"
)

// Config is to define how the Client behaves.
//
//   - Host is 127.0.0.1 in local environment.
//   - Port default value is 4001.
//   - Username and Password can be left to empty in local environment.
//     you can find them in GreptimeCloud service detail page.
//   - Database is the default database the client will operate on.
//     But you can change the database in InsertRequest or QueryRequest.
type Config struct {
	Host     string // no scheme or port included. example: 127.0.0.1
	Port     int    // default: 4001
	Username string
	Password string
	Database string // the default database

	// endpoints, when non-empty, overrides Host:Port. Each entry is a
	// "host:port" string. Populated via WithEndpoints.
	endpoints []string

	// picker selects an endpoint per RPC when more than one is configured.
	// Defaults to loadbalancer.NewRandom() at client construction time.
	picker loadbalancer.Picker

	tls     *options.TlsOption
	options []grpc.DialOption

	telemetry *options.TelemetryOptions
}

// NewConfig initializes a Config.
//
//   - NewConfig() — no endpoint yet; caller must call WithEndpoints.
//   - NewConfig("host") — single-endpoint legacy form; port defaults to
//     4001 and can be changed with WithPort.
//   - NewConfig("host:port") — single-endpoint shorthand equivalent to
//     NewConfig().WithEndpoints("host:port"). WithPort has no effect.
//   - NewConfig("host1:4001", "host2:4001", ...) — equivalent to
//     NewConfig().WithEndpoints(args...). WithPort has no effect.
func NewConfig(hosts ...string) *Config {
	cfg := &Config{
		Port: 4001,

		telemetry: options.NewTelemetryOptions(),
		options: []grpc.DialOption{
			options.NewUserAgentOption(version).Build(),
		},
	}
	switch len(hosts) {
	case 0:
		// nothing to do; caller will set endpoints via WithEndpoints.
	case 1:
		// If the argument parses as "host:port", treat it as an explicit
		// endpoint; otherwise keep the legacy bare-host semantics so that
		// NewConfig("127.0.0.1") and NewConfig("[::1]") both still work
		// with WithPort.
		if _, _, err := net.SplitHostPort(hosts[0]); err == nil {
			cfg.WithEndpoints(hosts[0])
		} else {
			cfg.Host = hosts[0]
		}
	default:
		cfg.WithEndpoints(hosts...)
	}
	return cfg
}

// WithPort set the Port field. Do not change it if you have no idea what it is.
func (c *Config) WithPort(port int) *Config {
	c.Port = port
	return c
}

// WithDatabase helps to specify the default database the client operates on.
func (c *Config) WithDatabase(database string) *Config {
	c.Database = database
	return c
}

// WithAuth helps to specify the Basic Auth username and password.
// Leave them empty if you are in local environment.
func (c *Config) WithAuth(username, password string) *Config {
	c.Username = username
	c.Password = password
	return c
}

// WithKeepalive helps to set the keepalive option.
//   - time. After a duration of this time if the client doesn't see any activity it
//     pings the server to see if the transport is still alive.
//     If set below 10s, a minimum value of 10s will be used instead.
//   - timeout. After having pinged for keepalive check, the client waits for a duration
//     of Timeout and if no activity is seen even after that the connection is closed.
func (c *Config) WithKeepalive(time, timeout time.Duration) *Config {
	keepalive := options.NewKeepaliveOption(time, timeout).Build()
	c.options = append(c.options, keepalive)
	return c
}

// TODO(yuanbohan): support more tls options
func (c *Config) WithInsecure(insecure bool) *Config {
	opt := options.NewTlsOption(insecure)
	c.tls = &opt
	return c
}

// WithMetricsEnabled enables/disables collection of SDK's metrics. Disabled by default.
func (c *Config) WithMetricsEnabled(b bool) *Config {
	c.telemetry.Metrics.Enabled = b
	return c
}

// WithMeterProvider provides a MeterProvider for SDK.
// If metrics collection is not enabled, then this option has no effect.
// If metrics collection is enabled and this option is not provide.
// the global MeterProvider will be used.
func (c *Config) WithMeterProvider(p metric.MeterProvider) *Config {
	c.telemetry.Metrics.MeterProvider = p
	return c
}

// WithTracesEnabled enables/disables collection of SDK's traces. Disabled by default.
func (c *Config) WithTracesEnabled(b bool) *Config {
	c.telemetry.Traces.Enabled = b
	return c
}

// WithTraceProvider provides a TracerProvider for SDK.
// If traces collection is not enabled, then this option has no effect.
// If traces collection is enabled and this option is not provide.
// the global MeterProvider will be used.
func (c *Config) WithTraceProvider(p trace.TracerProvider) *Config {
	c.telemetry.Traces.TracerProvider = p
	return c
}

// WithDialOption helps to specify the dial option
// which has not been supported by ingester sdk yet.
func (c *Config) WithDialOption(opt grpc.DialOption) *Config {
	c.options = append(c.options, opt)
	return c
}

// WithEndpoints configures multiple GreptimeDB endpoints for client-side
// load balancing. Each endpoint must be a "host:port" string. When at
// least one endpoint is provided, Host and Port are ignored. Passing no
// arguments is a no-op and the client falls back to Host:Port.
//
// Authentication, TLS, keepalive, telemetry and any dial options are
// shared across all endpoints; per-endpoint overrides are not supported.
func (c *Config) WithEndpoints(endpoints ...string) *Config {
	if len(endpoints) > 0 {
		c.endpoints = append([]string(nil), endpoints...)
	}
	return c
}

// WithLoadBalancer sets the load-balancing strategy used when more than one
// endpoint is configured. Defaults to loadbalancer.NewRandom() when unset.
// Has no observable effect when only a single endpoint is in use.
func (c *Config) WithLoadBalancer(picker loadbalancer.Picker) *Config {
	c.picker = picker
	return c
}

// resolveEndpoints returns the configured endpoints, falling back to
// [Host:Port] when WithEndpoints was not called. Returns an empty slice
// when neither a Host nor endpoints were set, which NewClient reports as a
// configuration error.
func (c *Config) resolveEndpoints() []string {
	if len(c.endpoints) > 0 {
		return c.endpoints
	}
	if c.Host == "" {
		return nil
	}
	return []string{fmt.Sprintf("%s:%d", c.Host, c.Port)}
}

func (c *Config) build() []grpc.DialOption {
	if c.tls == nil {
		opt := options.NewTlsOption(true)
		c.tls = &opt
	}

	c.options = append(c.options, c.tls.Build(), c.telemetry.Build())
	return c.options
}
