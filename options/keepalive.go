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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	defaultKeepaliveTime    = time.Second * 30
	defaultKeepaliveTimeout = time.Second * 5
)

type KeepaliveOption struct {
	time    time.Duration
	timeout time.Duration
}

func NewKeepaliveOption(time, timeout time.Duration) KeepaliveOption {
	return KeepaliveOption{
		time:    time,
		timeout: timeout,
	}
}

func (opt KeepaliveOption) Build() grpc.DialOption {
	param := keepalive.ClientParameters{
		PermitWithoutStream: true,
		Time:                defaultKeepaliveTime,
		Timeout:             defaultKeepaliveTimeout,
	}

	if opt.time != 0 {
		param.Time = opt.time
	}
	if opt.timeout != 0 {
		param.Timeout = opt.timeout
	}
	return grpc.WithKeepaliveParams(param)
}
