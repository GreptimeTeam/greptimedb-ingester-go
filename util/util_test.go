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

package util

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	gerr "github.com/GreptimeTeam/greptimedb-ingester-go/error"
)

func TestEmptyString(t *testing.T) {
	assert.True(t, IsEmptyString(""))
	assert.True(t, IsEmptyString(" "))
	assert.True(t, IsEmptyString("  "))
	assert.True(t, IsEmptyString("\t"))
}

func TestColumnName(t *testing.T) {
	key, err := ToColumnName("ts ")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = ToColumnName(" Ts")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = ToColumnName(" TS ")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = ToColumnName("DiskUsage ")
	assert.Nil(t, err)
	assert.Equal(t, "disk_usage", key)

	key, err = ToColumnName("Disk-Usage")
	assert.Nil(t, err)
	assert.Equal(t, "disk_usage", key)

	key, err = ToColumnName("   ")
	assert.NotNil(t, err)
	assert.Equal(t, "", key)

	key, err = ToColumnName(strings.Repeat("timestamp", 20))
	assert.NotNil(t, err)
	assert.Equal(t, "", key)
}

func TestPrecisionToDataType(t *testing.T) {
	_, err := PrecisionToDataType(123)
	assert.Equal(t, gerr.ErrInvalidTimePrecision, err)
}
