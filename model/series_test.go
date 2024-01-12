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

package model

import (
	"fmt"
	"testing"
	"time"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"
)

func TestSeries(t *testing.T) {
	s := Series{}
	assert.Nil(t, s.AddTag("Tag1", "tag val"))
	assert.Nil(t, s.AddTag("tag2 ", true))
	assert.Nil(t, s.AddTag(" tag3", int32(32)))
	assert.Nil(t, s.AddTag("tag4", float64(32.0)))
	timestamp := time.Now()
	assert.Nil(t, s.SetTimestamp(timestamp))
	assert.Nil(t, s.AddField("field1", []byte("field val")))
	assert.Nil(t, s.AddField("field2", float32(32.0)))
	assert.Nil(t, s.AddField("field3", uint8(8)))
	assert.Nil(t, s.AddField("field4", uint64(64)))

	// check columns
	assert.Equal(t, 8, len(s.columns))
	assert.Equal(t, greptimepb.ColumnDataType_STRING, s.columns["tag1"].typ)
	assert.Equal(t, greptimepb.SemanticType_TAG, s.columns["tag1"].semantic)
	assert.Equal(t, greptimepb.ColumnDataType_BOOLEAN, s.columns["tag2"].typ)
	assert.Equal(t, greptimepb.SemanticType_TAG, s.columns["tag2"].semantic)
	assert.Equal(t, greptimepb.ColumnDataType_INT32, s.columns["tag3"].typ)
	assert.Equal(t, greptimepb.SemanticType_TAG, s.columns["tag3"].semantic)
	assert.Equal(t, greptimepb.ColumnDataType_FLOAT64, s.columns["tag4"].typ)
	assert.Equal(t, greptimepb.SemanticType_TAG, s.columns["tag4"].semantic)
	assert.Equal(t, greptimepb.ColumnDataType_BINARY, s.columns["field1"].typ)
	assert.Equal(t, greptimepb.SemanticType_FIELD, s.columns["field1"].semantic)
	assert.Equal(t, greptimepb.ColumnDataType_FLOAT32, s.columns["field2"].typ)
	assert.Equal(t, greptimepb.SemanticType_FIELD, s.columns["field2"].semantic)
	assert.Equal(t, greptimepb.ColumnDataType_UINT8, s.columns["field3"].typ)
	assert.Equal(t, greptimepb.SemanticType_FIELD, s.columns["field3"].semantic)
	assert.Equal(t, greptimepb.ColumnDataType_UINT64, s.columns["field4"].typ)
	assert.Equal(t, greptimepb.SemanticType_FIELD, s.columns["field4"].semantic)

	// check values
	assert.Equal(t, 8, len(s.vals))
	assert.Equal(t, "tag val", s.vals["tag1"])
	assert.Equal(t, true, s.vals["tag2"])
	assert.Equal(t, int32(32), s.vals["tag3"])
	assert.Equal(t, float64(32.0), s.vals["tag4"])
	assert.Equal(t, []byte("field val"), s.vals["field1"])
	assert.Equal(t, float32(32.0), s.vals["field2"])
	assert.Equal(t, uint8(8), s.vals["field3"])
	assert.Equal(t, uint64(64), s.vals["field4"])

	// check timestamp
	assert.Equal(t, timestamp, s.timestamp)
}

func TestValueReplaced(t *testing.T) {
	s := Series{}
	val := "tag val"
	err := s.AddTag("tag1", val)
	assert.Nil(t, err)
	assert.Equal(t, val, s.vals["tag1"])

	newVal := "tag val again"
	err = s.AddTag("tag1", newVal)
	assert.Nil(t, err)
	assert.Equal(t, newVal, s.vals["tag1"])
}

func TestSeriesError(t *testing.T) {
	s := Series{}

	// type not match
	err := s.AddTag("tag1", "tag val")
	assert.Nil(t, err)
	err = s.AddTag("tag1", true)
	assert.NotNil(t, err)

	// tag and field contain same column
	err = s.AddTag("name", "tag val")
	assert.Nil(t, err)
	err = s.AddField("name", "field val")
	assert.NotNil(t, err)
}

func TestSeriesTypeNotMatch(t *testing.T) {
	s := &Series{}

	key := "int_tag"
	err := s.AddIntTag(key, 1)
	assert.Nil(t, err)

	err = s.AddFloatTag(key, 1)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Sprintf("the type of '%s' does not match: 'INT64' and 'FLOAT64'", key), err.Error())
}

func TestSeriesTagAndFieldCanNotContainSameKey(t *testing.T) {
	s := &Series{}

	key := "tag_column"
	err := s.AddIntTag(key, 1)
	assert.Nil(t, err)

	err = s.AddIntField(key, 1)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Sprintf("tag and field MUST NOT contain same key: %q", key), err.Error())

	// type checks before tag/field
	err = s.AddFloatField(key, 1)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Sprintf("the type of '%s' does not match: 'INT64' and 'FLOAT64'", key), err.Error())
}

func TestSeriesAddTypedTagAndField(t *testing.T) {
	s := &Series{}

	// tags
	err := s.AddIntTag("int_tag", 1)
	assert.Nil(t, err)

	err = s.AddFloatTag("float_tag", 1)
	assert.Nil(t, err)

	err = s.AddUintTag("uint_tag", 1)
	assert.Nil(t, err)

	err = s.AddBoolTag("bool_tag", true)
	assert.Nil(t, err)

	err = s.AddStringTag("string_tag", "1")
	assert.Nil(t, err)

	err = s.AddBytesTag("bytes_tag", []byte("1"))
	assert.Nil(t, err)

	// fields
	err = s.AddIntField("int_field", 1)
	assert.Nil(t, err)

	err = s.AddFloatField("float_field", 1)
	assert.Nil(t, err)

	err = s.AddUintField("uint_field", 1)
	assert.Nil(t, err)

	err = s.AddBoolField("bool_field", true)
	assert.Nil(t, err)

	err = s.AddStringField("string_field", "1")
	assert.Nil(t, err)

	err = s.AddBytesField("bytes_field", []byte("1"))
	assert.Nil(t, err)
}
