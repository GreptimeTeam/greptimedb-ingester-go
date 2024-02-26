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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var (
	uaOpt = grpc.WithUserAgent("greptimedb-ingester-go/" + Version)

	// TODO(yuanbohan): SecurityOptions
	insecureOpt = grpc.WithTransportCredentials(insecure.NewCredentials())

	defaultKeepaliveInterval = 30 * time.Second
	defaultKeepaliveTimeout  = 5 * time.Second
)

type Options struct {
	keepalive *KeepaliveOption
}

func NewOptions(keepalive *KeepaliveOption) *Options {
	return &Options{
		keepalive: keepalive,
	}
}

func (o *Options) WithKeepalive(keepalive *KeepaliveOption) *Options {
	o.keepalive = keepalive
	return o
}

func (o *Options) Build() []grpc.DialOption {
	options := []grpc.DialOption{uaOpt, insecureOpt}

	if o == nil {
		return options
	}

	if opt := o.keepalive.Build(); opt != nil {
		options = append(options, *opt)
	}

	return options
}

type KeepaliveOption struct {
	Interval time.Duration // default value is 30 seconds.
	Timeout  time.Duration // default value is 5 seconds.
}

func NewKeepaliveOptions() *KeepaliveOption {
	return &KeepaliveOption{
		Interval: defaultKeepaliveInterval,
		Timeout:  defaultKeepaliveTimeout,
	}
}

func (o *KeepaliveOption) WithInterval(d time.Duration) *KeepaliveOption {
	o.Interval = d
	return o
}

func (o *KeepaliveOption) WithTimeout(d time.Duration) *KeepaliveOption {
	o.Timeout = d
	return o
}

func (o *KeepaliveOption) Build() *grpc.DialOption {
	if o.Interval == 0 && o.Timeout == 0 {
		return nil
	}

	param := keepalive.ClientParameters{PermitWithoutStream: true}
	if o.Interval != 0 {
		param.Time = o.Interval
	}
	if o.Timeout != 0 {
		param.Timeout = o.Timeout
	}
	option := grpc.WithKeepaliveParams(param)

	return &option
}
