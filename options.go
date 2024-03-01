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
	"crypto/tls"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var (
	uaOpt       = grpc.WithUserAgent("greptimedb-ingester-go/" + version)
	secureOpt   = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: false}))
	insecureOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
)

type options struct {
	secure    *grpc.DialOption
	keepalive *grpc.DialOption
}

func newOptions() *options {
	return &options{
		secure: &insecureOpt,
	}
}

func (o *options) withKeepalive(interval, timeout time.Duration) *options {
	if interval == 0 && timeout == 0 {
		return o
	}

	param := keepalive.ClientParameters{PermitWithoutStream: true}
	if interval != 0 {
		param.Time = interval
	}
	if timeout != 0 {
		param.Timeout = timeout
	}
	opt := grpc.WithKeepaliveParams(param)
	o.keepalive = &opt
	return o
}

func (o *options) withSecure(secure bool) *options {
	if secure {
		o.secure = &secureOpt
	} else {
		o.secure = &insecureOpt
	}

	return o
}

func (o *options) build() []grpc.DialOption {
	opts := []grpc.DialOption{uaOpt}

	if o.keepalive != nil {
		opts = append(opts, *o.keepalive)
	}

	if o.secure != nil {
		opts = append(opts, *o.secure)
	}

	return opts
}
