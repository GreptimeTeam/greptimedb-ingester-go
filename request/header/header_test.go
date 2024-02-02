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

package header

import (
	"testing"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"

	"github.com/GreptimeTeam/greptimedb-ingester-go/errs"
)

func TestHeaderBuild(t *testing.T) {
	h := &Header{}

	gh, err := h.Build()
	assert.ErrorIs(t, err, errs.ErrEmptyDatabaseName)
	assert.Nil(t, gh)

	gh, err = h.WithDatabase("public").Build()
	assert.Nil(t, err)
	assert.Equal(t, &gpb.RequestHeader{Dbname: "public"}, gh)
	assert.Nil(t, gh.Authorization)

	gh, err = h.WithAuth("user", "pass").Build()
	assert.Nil(t, err)
	assert.Equal(t, &gpb.RequestHeader{Dbname: "public"}, gh)
	assert.NotNil(t, gh.Authorization)
}
