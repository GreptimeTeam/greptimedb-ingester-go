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

package greptime

import (
	"fmt"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"google.golang.org/grpc"

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

	tls     *options.TlsOption
	options []grpc.DialOption

	telemetry      *options.TelemetryOptions
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
}

// NewConfig helps to init Config with host only
func NewConfig(host string) *Config {
	return &Config{
		Host: host,
		Port: 4001,

		telemetry: options.NewTelemetryOptions(),
		options: []grpc.DialOption{
			options.NewUserAgentOption(version).Build(),
		},
	}
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
// If metrics colleciton is not enabled, then this option has no effect.
// If metrics colleciton is enabled and this option is not provide
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
// If traces colleciton is not enabled, then this option has no effect.
// If traces colleciton is enabled and this option is not provide
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

func (c *Config) endpoint() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func (c *Config) build() []grpc.DialOption {
	if c.tls == nil {
		opt := options.NewTlsOption(true)
		c.tls = &opt
	}

	c.options = append(c.options, c.tls.Build(), c.telemetry.Build())
	return c.options
}
