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
	"testing"
	"time"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"

	gerr "github.com/GreptimeTeam/greptimedb-ingester-go/error"
)

func TestMetric(t *testing.T) {
	s := Series{}
	s.AddTag("tag1", "tag val")
	s.AddTag("tag2", true)
	s.AddTag("tag3", int32(32))
	s.AddTag("tag4", float64(32.0))
	s.AddField("field1", []byte("field val"))
	s.AddField("field2", float32(32.0))
	s.AddField("field3", uint8(8))
	s.AddField("field4", uint64(64))
	s.SetTimestamp(time.Now())

	m := Metric{}
	err := m.AddSeries(s)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(m.GetSeries()))
}

func TestMetricTypeNotMatch(t *testing.T) {
	s1 := Series{}
	s1.AddTag("tag", "tag val")
	s1.SetTimestamp(time.Now())

	s2 := Series{}
	s2.AddTag("tag", true)
	s2.SetTimestamp(time.Now())

	m := Metric{}
	err := m.AddSeries(s1)
	assert.Nil(t, err)

	err = m.AddSeries(s2)
	assert.NotNil(t, err)
}

func TestMetricSemanticNotMatch(t *testing.T) {
	s1 := Series{}
	s1.AddTag("name", "tag val")
	s1.SetTimestamp(time.Now())

	s2 := Series{}
	s2.AddField("name", true)
	s2.SetTimestamp(time.Now())

	m := Metric{}
	err := m.AddSeries(s1)
	assert.Nil(t, err)

	err = m.AddSeries(s2)
	assert.NotNil(t, err)
}

// 9 columns
// row1: 4 tags, 2 fields (with 2 null column), 1 timestamp, named as default "ts"
// row2: 2 tags, 4 fields (with 2 null column), 1 timestamp, named as default "ts"
// the timestamp column should be at last
func TestGreptimeColumn(t *testing.T) {
	timestamp := time.Now()

	s1 := Series{}
	s1.AddTag(" tag1", "tag1")
	s1.AddTag("tag2 ", true)
	s1.AddTag("Tag3", int32(32))
	s1.AddTag("TAG4", float64(32.0))
	s1.AddField("Field1", uint8(8))
	s1.AddField("FIELD2", uint64(64))
	s1.SetTimestamp(timestamp)

	s2 := Series{}
	s2.AddTag("tag1", "tag2")
	s2.AddTag("tag2", false)
	s2.AddField("field1", uint8(8))
	s2.AddField("field2", uint64(64))
	s2.AddField("fieldName3", []byte("field3"))
	s2.AddField("Field-Name4", float32(32.0))
	s2.SetTimestamp(timestamp)

	m := Metric{}
	assert.Nil(t, m.AddSeries(s1))
	assert.Nil(t, m.AddSeries(s2))

	cols, err := m.IntoGreptimeColumn()
	assert.Nil(t, err)
	assert.Equal(t, 9, len(cols))

	col1 := cols[0]
	assert.Equal(t, "tag1", col1.ColumnName)
	assert.Equal(t, greptimepb.ColumnDataType_STRING, col1.Datatype)
	assert.Equal(t, greptimepb.SemanticType_TAG, col1.SemanticType)
	assert.Equal(t, []string{"tag1", "tag2"}, col1.Values.StringValues)
	assert.Empty(t, col1.NullMask)

	col2 := cols[1]
	assert.Equal(t, "tag2", col2.ColumnName)
	assert.Equal(t, greptimepb.ColumnDataType_BOOLEAN, col2.Datatype)
	assert.Equal(t, greptimepb.SemanticType_TAG, col2.SemanticType)
	assert.Equal(t, []bool{true, false}, col2.Values.BoolValues)
	assert.Empty(t, col2.NullMask)

	col3 := cols[2]
	assert.Equal(t, "tag3", col3.ColumnName)
	assert.Equal(t, greptimepb.ColumnDataType_INT32, col3.Datatype)
	assert.Equal(t, greptimepb.SemanticType_TAG, col3.SemanticType)
	assert.Equal(t, []int32{32}, col3.Values.I32Values)
	assert.Equal(t, []byte{2}, col3.NullMask)

	col4 := cols[3]
	assert.Equal(t, "tag4", col4.ColumnName)
	assert.Equal(t, greptimepb.ColumnDataType_FLOAT64, col4.Datatype)
	assert.Equal(t, greptimepb.SemanticType_TAG, col4.SemanticType)
	assert.Equal(t, []float64{32}, col4.Values.F64Values)
	assert.Equal(t, []byte{2}, col4.NullMask)

	col5 := cols[4]
	assert.Equal(t, "field1", col5.ColumnName)
	assert.Equal(t, greptimepb.ColumnDataType_UINT8, col5.Datatype)
	assert.Equal(t, greptimepb.SemanticType_FIELD, col5.SemanticType)
	assert.Equal(t, []uint32{8, 8}, col5.Values.U8Values)
	assert.Empty(t, col5.NullMask)

	col6 := cols[5]
	assert.Equal(t, "field2", col6.ColumnName)
	assert.Equal(t, greptimepb.ColumnDataType_UINT64, col6.Datatype)
	assert.Equal(t, greptimepb.SemanticType_FIELD, col6.SemanticType)
	assert.Equal(t, []uint64{64, 64}, col6.Values.U64Values)
	assert.Empty(t, col6.NullMask)

	col7 := cols[6]
	assert.Equal(t, "field_name3", col7.ColumnName)
	assert.Equal(t, greptimepb.ColumnDataType_BINARY, col7.Datatype)
	assert.Equal(t, greptimepb.SemanticType_FIELD, col7.SemanticType)
	assert.Equal(t, [][]byte{[]byte("field3")}, col7.Values.BinaryValues)
	assert.Equal(t, []byte{1}, col7.NullMask)

	col8 := cols[7]
	assert.Equal(t, "field_name4", col8.ColumnName)
	assert.Equal(t, greptimepb.ColumnDataType_FLOAT32, col8.Datatype)
	assert.Equal(t, greptimepb.SemanticType_FIELD, col8.SemanticType)
	assert.Equal(t, []float32{32}, col8.Values.F32Values)
	assert.Equal(t, []byte{1}, col8.NullMask)

	col9 := cols[8]
	assert.Equal(t, "ts", col9.ColumnName)
	assert.Equal(t, greptimepb.ColumnDataType_TIMESTAMP_MILLISECOND, col9.Datatype)
	assert.Equal(t, greptimepb.SemanticType_TIMESTAMP, col9.SemanticType)
	assert.Equal(t, []int64{timestamp.UnixMilli(), timestamp.UnixMilli()}, col9.Values.TimestampMillisecondValues)
	assert.Empty(t, col9.NullMask)
}

func TestWithoutTimestamp(t *testing.T) {
	series := Series{}
	metric := Metric{}
	err := metric.AddSeries(series) // timestamp can be nil for querying
	assert.Nil(t, err)
}

func TestSetColumn(t *testing.T) {
	testCases := []struct {
		name     string
		col      *greptimepb.Column
		val      interface{}
		expected *greptimepb.Column
	}{
		{
			name: "set int8 value",
			col: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_INT8,
				Values: &greptimepb.Column_Values{
					I8Values: []int32{1, 2, 3},
				},
			},
			val: int8(4),
			expected: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_INT8,
				Values: &greptimepb.Column_Values{
					I8Values: []int32{1, 2, 3, 4},
				},
			},
		},
		{
			name: "set int16 value",
			col: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_INT16,
				Values: &greptimepb.Column_Values{
					I16Values: []int32{1, 2, 3},
				},
			},
			val: int16(4),
			expected: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_INT16,
				Values: &greptimepb.Column_Values{
					I16Values: []int32{1, 2, 3, 4},
				},
			},
		},
		{
			name: "set uint8 value",
			col: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_UINT8,
				Values: &greptimepb.Column_Values{
					U8Values: []uint32{1, 2, 3},
				},
			},
			val: uint8(4),
			expected: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_UINT8,
				Values: &greptimepb.Column_Values{
					U8Values: []uint32{1, 2, 3, 4},
				},
			},
		},
		{
			name: "set uint16 value",
			col: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_UINT16,
				Values: &greptimepb.Column_Values{
					U16Values: []uint32{1, 2, 3},
				},
			},
			val: uint16(4),
			expected: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_UINT16,
				Values: &greptimepb.Column_Values{
					U16Values: []uint32{1, 2, 3, 4},
				},
			},
		},

		{
			name: "set float32 value",
			col: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_FLOAT32,
				Values: &greptimepb.Column_Values{
					F32Values: []float32{1.0, 2.0, 3.0},
				},
			},
			val: float32(4.0),
			expected: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_FLOAT32,
				Values: &greptimepb.Column_Values{
					F32Values: []float32{1.0, 2.0, 3.0, 4.0},
				},
			},
		},
		{
			name: "set binary value",
			col: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_BINARY,
				Values: &greptimepb.Column_Values{
					BinaryValues: [][]byte{[]byte("hello")},
				},
			},
			val: []byte("world"),
			expected: &greptimepb.Column{
				Datatype: greptimepb.ColumnDataType_BINARY,
				Values: &greptimepb.Column_Values{
					BinaryValues: [][]byte{[]byte("hello"), []byte("world")},
				},
			},
		},
	}

	for _, cas := range testCases {
		err := setColumn(cas.col, cas.val)
		assert.Nil(t, err)
		assert.Equal(t, cas.expected, cas.col)
	}

	errCol := &greptimepb.Column{
		Datatype: greptimepb.ColumnDataType(99),
	}
	err := setColumn(errCol, "wrong")
	assert.Equal(t, "unknown column data type: 99", err.Error())
}

func TestSetTimePrecision(t *testing.T) {
	m := Metric{}
	err := m.SetTimePrecision(123)
	assert.Equal(t, gerr.ErrInvalidTimePrecision, err)
}

func TestSetTimeAlias(t *testing.T) {
	m := Metric{}
	err := m.SetTimestampAlias("")
	assert.Equal(t, gerr.ErrEmptyKey, err)
}

func TestGetTags(t *testing.T) {
	s := Series{}
	s.AddTag("t1", "tag val")
	s.AddTag("t2 ", true)
	s.AddField("f1", int32(32))
	timestamp := time.UnixMilli(245235234523)
	s.SetTimestamp(timestamp)
	assert.Equal(t, []string{"t1", "t2", "f1"}, s.GetTagsAndFields())

	m := Metric{}
	m.AddSeries(s)
	assert.Equal(t, []string{"t1", "t2", "f1"}, m.GetTagsAndFields())
}
