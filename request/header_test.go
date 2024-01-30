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

package request

import (
	"testing"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"

	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
	"github.com/GreptimeTeam/greptimedb-ingester-go/errs"
)

func TestHeaderBuild(t *testing.T) {
	h := &reqHeader{}

	gh, err := h.build(&config.Config{})
	assert.ErrorIs(t, err, errs.ErrEmptyDatabaseName)
	assert.Nil(t, gh)

	gh, err = h.build(&config.Config{Database: "database"})
	assert.Nil(t, err)
	assert.Equal(t, &greptimepb.RequestHeader{
		Dbname: "database",
	}, gh)

	h.database = "db_in_header"
	gh, err = h.build(&config.Config{Database: "database"})
	assert.Nil(t, err)
	assert.Equal(t, &greptimepb.RequestHeader{
		Dbname: "db_in_header",
	}, gh)
}
