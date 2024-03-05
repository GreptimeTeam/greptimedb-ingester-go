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
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type TlsOption struct {
	InsecureSkipVerify bool

	// TODO(yuanbohan): support cert path
	// ServerCertPath                string //
	// ClientKeyPath, ClientCertPath string // mTLS
}

func NewTlsOption(InsecureSkipVerify bool) TlsOption {
	return TlsOption{InsecureSkipVerify: InsecureSkipVerify}
}

func (opt TlsOption) Build() grpc.DialOption {
	if opt.InsecureSkipVerify {
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		// TODO(yuanbohan): setting for cert or key
		tls := tls.Config{InsecureSkipVerify: opt.InsecureSkipVerify}
		return grpc.WithTransportCredentials(credentials.NewTLS(&tls))
	}
}
