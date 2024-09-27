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

package context

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const (
	hintPrefix = "x-greptime-hint-"
)

type Hint struct {
	Key   string
	Value string
}

type Option func(ctx context.Context) context.Context

func New(parent context.Context, opts ...Option) context.Context {
	ctx := parent
	for _, opt := range opts {
		ctx = opt(parent)
	}
	return ctx
}

func WithHints(hints []*Hint) Option {
	return func(ctx context.Context) context.Context {
		md := metadata.New(nil)
		for _, hint := range hints {
			md.Append(hintPrefix+hint.Key, hint.Value)
		}
		return metadata.NewOutgoingContext(ctx, md)
	}
}
