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
	hintKeyPrefix = "x-greptime-hint-"
	hintsPrefix   = "x-greptime-hints"
)

type Hint struct {
	Key   string
	Value string
}

type Option func(ctx context.Context) context.Context

func New(parent context.Context, opts ...Option) context.Context {
	ctx := parent
	for _, opt := range opts {
		ctx = opt(ctx)
	}
	return ctx
}

// WithHint formats hints as: 'x-greptime-hint-key: value'.
func WithHint(hints []*Hint) Option {
	return func(ctx context.Context) context.Context {
		md := metadata.New(nil)
		for _, hint := range hints {
			md.Append(hintKeyPrefix+hint.Key, hint.Value)
		}
		return metadata.NewOutgoingContext(ctx, md)
	}
}

// WithHints formats hints as: 'x-greptime-hints: key1=value1,key2=value2'.
func WithHints(hints string) Option {
	return func(ctx context.Context) context.Context {
		md := metadata.New(nil)
		md.Append(hintsPrefix, hints)
		return metadata.NewOutgoingContext(ctx, md)
	}
}
