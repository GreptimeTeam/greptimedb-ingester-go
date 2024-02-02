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

package config

import (
	"fmt"

	"google.golang.org/grpc"
)

// Config is to define how the Client behaves.
//
//   - Host is 127.0.0.1 in local environment.
//   - Port default value is 4001.
//   - Username and Password can be left to empty in local environment.
//     you can find them in GreptimeCloud service detail page.
//   - Database is the default database the client will operate on.
//     But you can change the database in InsertRequest or QueryRequest.
//   - DialOptions and CallOptions are for gRPC service.
//     You can specify them or leave them empty.
type Config struct {
	Host     string // no scheme or port included. example: 127.0.0.1
	Port     int    // default: 4001
	Username string
	Password string
	Database string // the default database

	// DialOptions are passed to grpc.DialContext
	// when a new gRPC connection is to be created.
	DialOptions []grpc.DialOption

	// CallOptions are passed to StreamClient
	CallOptions []grpc.CallOption
}

// New helps to init Config with host only
func New(host string) *Config {
	return &Config{
		Host: host,
		Port: 4001,

		DialOptions: []grpc.DialOption{
			grpc.WithUserAgent("greptimedb-ingester-go"),
		},

		CallOptions: []grpc.CallOption{},
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

// WithAuth helps to specify the Basic Auth username and password
func (c *Config) WithAuth(username, password string) *Config {
	c.Username = username
	c.Password = password
	return c
}

func (c *Config) WithDialOptions(options ...grpc.DialOption) *Config {
	if c.DialOptions == nil {
		c.DialOptions = []grpc.DialOption{}
	}

	c.DialOptions = append(c.DialOptions, options...)
	return c
}

func (c *Config) WithCallOptions(options ...grpc.CallOption) *Config {
	if c.CallOptions == nil {
		c.CallOptions = []grpc.CallOption{}
	}

	c.CallOptions = append(c.CallOptions, options...)
	return c
}

func (c *Config) GetEndpoint() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
