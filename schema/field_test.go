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

		// the following are the same

		BS1 []byte `greptime:"type:bytes"`
		BS2 []byte `greptime:"type:binary"`

		B1 bool `greptime:"type:bool"`
		B2 bool `greptime:"type:boolean"`

		F1 float64 `greptime:"type:float"`
		F2 float64 `greptime:"type:float64"`

		I1 int64 `greptime:"type:int"`
		I2 int64 `greptime:"type:int64"`

		U1 uint64 `greptime:"type:uint"`
		U2 uint64 `greptime:"type:uint64"`
	}

	{ // timestamp
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

	}

	{ // bytes
		{
			bs1, ok := reflect.TypeOf(Struct{}).FieldByName("BS1")
			assert.True(t, ok)
			field, err := parseField(bs1)
			assert.Nil(t, err)
			assert.EqualValues(t, newField("bs1", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), field)
		}

		{
			bs2, ok := reflect.TypeOf(Struct{}).FieldByName("BS2")
			assert.True(t, ok)
			field, err := parseField(bs2)
			assert.Nil(t, err)
			assert.EqualValues(t, newField("bs2", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), field)
		}
	}

	{ // bool
		{
			b1, ok := reflect.TypeOf(Struct{}).FieldByName("B1")
			assert.True(t, ok)
			field, err := parseField(b1)
			assert.Nil(t, err)
			assert.EqualValues(t, newField("b1", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), field)
		}

		{
			b2, ok := reflect.TypeOf(Struct{}).FieldByName("B2")
			assert.True(t, ok)
			field, err := parseField(b2)
			assert.Nil(t, err)
			assert.EqualValues(t, newField("b2", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), field)
		}
	}

	{ // float
		{
			f1, ok := reflect.TypeOf(Struct{}).FieldByName("F1")
			assert.True(t, ok)
			field, err := parseField(f1)
			assert.Nil(t, err)
			assert.EqualValues(t, newField("f1", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), field)
		}

		{
			f2, ok := reflect.TypeOf(Struct{}).FieldByName("F2")
			assert.True(t, ok)
			field, err := parseField(f2)
			assert.Nil(t, err)
			assert.EqualValues(t, newField("f2", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), field)
		}
	}

	{ // int
		{
			I1, ok := reflect.TypeOf(Struct{}).FieldByName("I1")
			assert.True(t, ok)
			field, err := parseField(I1)
			assert.Nil(t, err)
			assert.EqualValues(t, newField("i1", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), field)
		}

		{
			i2, ok := reflect.TypeOf(Struct{}).FieldByName("I2")
			assert.True(t, ok)
			field, err := parseField(i2)
			assert.Nil(t, err)
			assert.EqualValues(t, newField("i2", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), field)
		}
	}

	{ // uint
		{
			U1, ok := reflect.TypeOf(Struct{}).FieldByName("U1")
			assert.True(t, ok)
			field, err := parseField(U1)
			assert.Nil(t, err)
			assert.EqualValues(t, newField("u1", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), field)
		}

		{
			u2, ok := reflect.TypeOf(Struct{}).FieldByName("U2")
			assert.True(t, ok)
			field, err := parseField(u2)
			assert.Nil(t, err)
			assert.EqualValues(t, newField("u2", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), field)
		}
	}
}
