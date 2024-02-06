// Copyright 2024 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"reflect"
	"testing"
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"
)

func TestIsTimeType(t *testing.T) {
	assert.False(t, isTimeType(reflect.TypeOf(1)))
	assert.False(t, isTimeType(reflect.TypeOf("string")))

	var s []int
	assert.False(t, isTimeType(reflect.TypeOf(s)))

	assert.True(t, isTimeType(reflect.TypeOf(time.Now())))
}

func TestParseSpecialTypeTag(t *testing.T) {
	type Struct struct {
		T1 time.Time `greptime:"type:timestamp;precision:second"`
		T2 time.Time `greptime:"type:timestamp;precision:millisecond"`
		T3 time.Time `greptime:"type:timestamp;precision:microsecond"`
		T4 time.Time `greptime:"type:timestamp;precision:nanosecond"`

		B1 []byte `greptime:"type=timestamp,precision=bytes"`
		B2 []byte `greptime:"type=timestamp,precision=binary"`
	}

	{
		t1, ok := reflect.TypeOf(Struct{}).FieldByName("T1")
		assert.True(t, ok)
		field, err := parseField(t1)
		assert.Nil(t, err)
		assert.EqualValues(t, newField("t1", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_SECOND), field)
	}

	{
		t2, ok := reflect.TypeOf(Struct{}).FieldByName("T2")
		assert.True(t, ok)
		field, err := parseField(t2)
		assert.Nil(t, err)
		assert.EqualValues(t, newField("t2", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), field)
	}

	{
		t3, ok := reflect.TypeOf(Struct{}).FieldByName("T3")
		assert.True(t, ok)
		field, err := parseField(t3)
		assert.Nil(t, err)
		assert.EqualValues(t, newField("t3", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MICROSECOND), field)
	}

	{
		t4, ok := reflect.TypeOf(Struct{}).FieldByName("T4")
		assert.True(t, ok)
		field, err := parseField(t4)
		assert.Nil(t, err)
		assert.EqualValues(t, newField("t4", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_NANOSECOND), field)
	}

	{
		b1, ok := reflect.TypeOf(Struct{}).FieldByName("B1")
		assert.True(t, ok)
		field, err := parseField(b1)
		assert.Nil(t, err)
		assert.EqualValues(t, newField("b1", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), field)
	}

	{
		b2, ok := reflect.TypeOf(Struct{}).FieldByName("B2")
		assert.True(t, ok)
		field, err := parseField(b2)
		assert.Nil(t, err)
		assert.EqualValues(t, newField("b2", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), field)
	}
}
