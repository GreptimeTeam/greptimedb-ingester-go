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
	"testing"
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"
)

func TestParseSchemaWithoutTags(t *testing.T) {

	assertSchema := func(t *testing.T, schema Schema) {
		assert.Equal(t, "monitor", schema.TableName)
		assert.Len(t, schema.Fields, 32)

		assert.EqualValues(t, newField("int", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.Fields[0])
		assert.EqualValues(t, newField("int8", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT8), schema.Fields[1])
		assert.EqualValues(t, newField("int16", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT16), schema.Fields[2])
		assert.EqualValues(t, newField("int32", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), schema.Fields[3])
		assert.EqualValues(t, newField("int64", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.Fields[4])
		assert.EqualValues(t, newField("uint", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.Fields[5])
		assert.EqualValues(t, newField("uint8", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT8), schema.Fields[6])
		assert.EqualValues(t, newField("uint16", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT16), schema.Fields[7])
		assert.EqualValues(t, newField("uint32", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT32), schema.Fields[8])
		assert.EqualValues(t, newField("uint64", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.Fields[9])
		assert.EqualValues(t, newField("float32", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT32), schema.Fields[10])
		assert.EqualValues(t, newField("float64", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), schema.Fields[11])
		assert.EqualValues(t, newField("boolean", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), schema.Fields[12])
		assert.EqualValues(t, newField("binary", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), schema.Fields[13])
		assert.EqualValues(t, newField("string", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING), schema.Fields[14])
		assert.EqualValues(t, newField("date", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), schema.Fields[15])

		offset := 16
		assert.EqualValues(t, newField("ptr_int", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.Fields[0+offset])
		assert.EqualValues(t, newField("ptr_int8", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT8), schema.Fields[1+offset])
		assert.EqualValues(t, newField("ptr_int16", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT16), schema.Fields[2+offset])
		assert.EqualValues(t, newField("ptr_int32", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), schema.Fields[3+offset])
		assert.EqualValues(t, newField("ptr_int64", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.Fields[4+offset])
		assert.EqualValues(t, newField("ptr_uint", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.Fields[5+offset])
		assert.EqualValues(t, newField("ptr_uint8", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT8), schema.Fields[6+offset])
		assert.EqualValues(t, newField("ptr_uint16", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT16), schema.Fields[7+offset])
		assert.EqualValues(t, newField("ptr_uint32", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT32), schema.Fields[8+offset])
		assert.EqualValues(t, newField("ptr_uint64", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.Fields[9+offset])
		assert.EqualValues(t, newField("ptr_float32", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT32), schema.Fields[10+offset])
		assert.EqualValues(t, newField("ptr_float64", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), schema.Fields[11+offset])
		assert.EqualValues(t, newField("ptr_boolean", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), schema.Fields[12+offset])
		assert.EqualValues(t, newField("ptr_binary", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), schema.Fields[13+offset])
		assert.EqualValues(t, newField("ptr_string", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING), schema.Fields[14+offset])
		assert.EqualValues(t, newField("ptr_date", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), schema.Fields[15+offset])
	}

	type Monitor struct {
		INT     int
		INT8    int8
		INT16   int16
		INT32   int32
		INT64   int64
		UINT    uint
		UINT8   uint8
		UINT16  uint16
		UINT32  uint32
		UINT64  uint64
		FLOAT32 float32
		FLOAT64 float64
		BOOLEAN bool
		BINARY  []byte
		STRING  string
		DATE    time.Time

		PtrINT     *int
		PtrINT8    *int8
		PtrINT16   *int16
		PtrINT32   *int32
		PtrINT64   *int64
		PtrUINT    *uint
		PtrUINT8   *uint8
		PtrUINT16  *uint16
		PtrUINT32  *uint32
		PtrUINT64  *uint64
		PtrFLOAT32 *float32
		PtrFLOAT64 *float64
		PtrBOOLEAN *bool
		PtrBINARY  *[]byte
		PtrSTRING  *string
		PtrDATE    *time.Time

		privateField string // will be ignored
	}

	schema, err := ParseSchema(Monitor{privateField: "private"})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	schema, err = ParseSchema(&Monitor{privateField: "private"})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	schema, err = ParseSchema([]Monitor{{privateField: "private"}})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	var monitor *Monitor
	schema, err = ParseSchema(monitor)
	assert.Nil(t, err)
	assertSchema(t, *schema)
}

func TestParseSchemaWithTags(t *testing.T) {

	assertSchema := func(t *testing.T, schema Schema) {
		assert.Equal(t, "monitor", schema.TableName)
		assert.Len(t, schema.Fields, 42)

		assert.EqualValues(t, newField("int_column", gpb.SemanticType_TAG, gpb.ColumnDataType_INT64), schema.Fields[0])
		assert.EqualValues(t, newField("int8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT8), schema.Fields[1])
		assert.EqualValues(t, newField("int16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT16), schema.Fields[2])
		assert.EqualValues(t, newField("int32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), schema.Fields[3])
		assert.EqualValues(t, newField("int64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.Fields[4])
		assert.EqualValues(t, newField("uint_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.Fields[5])
		assert.EqualValues(t, newField("uint8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT8), schema.Fields[6])
		assert.EqualValues(t, newField("uint16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT16), schema.Fields[7])
		assert.EqualValues(t, newField("uint32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT32), schema.Fields[8])
		assert.EqualValues(t, newField("uint64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.Fields[9])
		assert.EqualValues(t, newField("float32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT32), schema.Fields[10])
		assert.EqualValues(t, newField("float64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), schema.Fields[11])
		assert.EqualValues(t, newField("boolean_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), schema.Fields[12])
		assert.EqualValues(t, newField("binary_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), schema.Fields[13])
		assert.EqualValues(t, newField("string_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING), schema.Fields[14])
		assert.EqualValues(t, newField("date_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATE), schema.Fields[15])
		assert.EqualValues(t, newField("datetime_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATETIME), schema.Fields[16])
		assert.EqualValues(t, newField("timestamp_second_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_SECOND), schema.Fields[17])
		assert.EqualValues(t, newField("timestamp_millisecond_column", gpb.SemanticType_TIMESTAMP, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), schema.Fields[18])
		assert.EqualValues(t, newField("timestamp_microsecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MICROSECOND), schema.Fields[19])
		assert.EqualValues(t, newField("timestamp_nanosecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_NANOSECOND), schema.Fields[20])

		offset := 21
		assert.EqualValues(t, newField("ptr_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.Fields[0+offset])
		assert.EqualValues(t, newField("ptr_int8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT8), schema.Fields[1+offset])
		assert.EqualValues(t, newField("ptr_int16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT16), schema.Fields[2+offset])
		assert.EqualValues(t, newField("ptr_int32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), schema.Fields[3+offset])
		assert.EqualValues(t, newField("ptr_int64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.Fields[4+offset])
		assert.EqualValues(t, newField("ptr_uint_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.Fields[5+offset])
		assert.EqualValues(t, newField("ptr_uint8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT8), schema.Fields[6+offset])
		assert.EqualValues(t, newField("ptr_uint16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT16), schema.Fields[7+offset])
		assert.EqualValues(t, newField("ptr_uint32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT32), schema.Fields[8+offset])
		assert.EqualValues(t, newField("ptr_uint64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.Fields[9+offset])
		assert.EqualValues(t, newField("ptr_float32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT32), schema.Fields[10+offset])
		assert.EqualValues(t, newField("ptr_float64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), schema.Fields[11+offset])
		assert.EqualValues(t, newField("ptr_boolean_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), schema.Fields[12+offset])
		assert.EqualValues(t, newField("ptr_binary_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), schema.Fields[13+offset])
		assert.EqualValues(t, newField("ptr_string_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING), schema.Fields[14+offset])
		assert.EqualValues(t, newField("ptr_date_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATE), schema.Fields[15+offset])
		assert.EqualValues(t, newField("ptr_datetime_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATETIME), schema.Fields[16+offset])
		assert.EqualValues(t, newField("ptr_timestamp_second_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_SECOND), schema.Fields[17+offset])
		assert.EqualValues(t, newField("ptr_timestamp_millisecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), schema.Fields[18+offset])
		assert.EqualValues(t, newField("ptr_timestamp_microsecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MICROSECOND), schema.Fields[19+offset])
		assert.EqualValues(t, newField("ptr_timestamp_nanosecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_NANOSECOND), schema.Fields[20+offset])
	}

	type Monitor struct {
		INT                   int       `greptime:"tag;column:int_column;type:int"`
		INT8                  int8      `greptime:"field;column:int8_column;type:int8"`
		INT16                 int16     `greptime:"field;column:int16_column;type:int16"`
		INT32                 int32     `greptime:"field;column:int32_column;type:int32"`
		INT64                 int64     `greptime:"field;column:int64_column;type:int64"`
		UINT                  uint      `greptime:"field;column:uint_column;type:uint"`
		UINT8                 uint8     `greptime:"field;column:uint8_column;type:uint8"`
		UINT16                uint16    `greptime:"field;column:uint16_column;type:uint16"`
		UINT32                uint32    `greptime:"field;column:uint32_column;type:uint32"`
		UINT64                uint64    `greptime:"field;column:uint64_column;type:uint64"`
		FLOAT32               float32   `greptime:"field;column:float32_column;type:float32"`
		FLOAT64               float64   `greptime:"field;column:float64_column;type:float64"`
		BOOLEAN               bool      `greptime:"field;column:boolean_column;type:boolean"`
		BINARY                []byte    `greptime:"field;column:binary_column;type:binary"`
		STRING                string    `greptime:"field;column:string_column;type:string"`
		DATE                  time.Time `greptime:"field;column:date_column;type:date"`
		DATETIME              time.Time `greptime:"field;column:datetime_column;type:datetime"`
		TIMESTAMP_SECOND      time.Time `greptime:"field;column:timestamp_second_column;type:timestamp;precision:second"`
		TIMESTAMP_MILLISECOND time.Time `greptime:"timestamp;column:timestamp_millisecond_column;type:timestamp;precision:millisecond"`
		TIMESTAMP_MICROSECOND time.Time `greptime:"field;column:timestamp_microsecond_column;type:timestamp;precision:microsecond"`
		TIMESTAMP_NANOSECOND  time.Time `greptime:"field;column:timestamp_nanosecond_column;type:timestamp;precision:nanosecond"`

		PtrINT                   *int       `greptime:"field;column:ptr_int_column;type:int"`
		PtrINT8                  *int8      `greptime:"field;column:ptr_int8_column;type:int8"`
		PtrINT16                 *int16     `greptime:"field;column:ptr_int16_column;type:int16"`
		PtrINT32                 *int32     `greptime:"field;column:ptr_int32_column;type:int32"`
		PtrINT64                 *int64     `greptime:"field;column:ptr_int64_column;type:int64"`
		PtrUINT                  *uint      `greptime:"field;column:ptr_uint_column;type:uint"`
		PtrUINT8                 *uint8     `greptime:"field;column:ptr_uint8_column;type:uint8"`
		PtrUINT16                *uint16    `greptime:"field;column:ptr_uint16_column;type:uint16"`
		PtrUINT32                *uint32    `greptime:"field;column:ptr_uint32_column;type:uint32"`
		PtrUINT64                *uint64    `greptime:"field;column:ptr_uint64_column;type:uint64"`
		PtrFLOAT32               *float32   `greptime:"field;column:ptr_float32_column;type:float32"`
		PtrFLOAT64               *float64   `greptime:"field;column:ptr_float64_column;type:float64"`
		PtrBOOLEAN               *bool      `greptime:"field;column:ptr_boolean_column;type:boolean"`
		PtrBINARY                *[]byte    `greptime:"field;column:ptr_binary_column;type:binary"`
		PtrSTRING                *string    `greptime:"field;column:ptr_string_column;type:string"`
		PtrDATE                  *time.Time `greptime:"field;column:ptr_date_column;type:date"`
		PtrDATETIME              *time.Time `greptime:"field;column:ptr_datetime_column;type:datetime"`
		PtrTIMESTAMP_SECOND      *time.Time `greptime:"field;column:ptr_timestamp_second_column;type:timestamp;precision:second"`
		PtrTIMESTAMP_MILLISECOND *time.Time `greptime:"field;column:ptr_timestamp_millisecond_column;type:timestamp;precision:millisecond"`
		PtrTIMESTAMP_MICROSECOND *time.Time `greptime:"field;column:ptr_timestamp_microsecond_column;type:timestamp;precision:microsecond"`
		PtrTIMESTAMP_NANOSECOND  *time.Time `greptime:"field;column:ptr_timestamp_nanosecond_column;type:timestamp;precision:nanosecond"`

		privateField string // will be ignored
	}

	schema, err := ParseSchema(Monitor{privateField: "private"})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	schema, err = ParseSchema(&Monitor{privateField: "private"})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	schema, err = ParseSchema([]Monitor{{privateField: "private"}})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	var monitor *Monitor
	schema, err = ParseSchema(monitor)
	assert.Nil(t, err)
	assertSchema(t, *schema)
}

func TestParseTimestampViaIntType(t *testing.T) {

	assertSchema := func(t *testing.T, schema Schema) {
		assert.Equal(t, "monitor", schema.TableName)
		assert.Len(t, schema.Fields, 6)

		assert.EqualValues(t, newField("date_column", gpb.SemanticType_TAG, gpb.ColumnDataType_DATE), schema.Fields[0])
		assert.EqualValues(t, newField("datetime_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATETIME), schema.Fields[1])
		assert.EqualValues(t, newField("timestamp_second_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_SECOND), schema.Fields[2])
		assert.EqualValues(t, newField("timestamp_millisecond_column", gpb.SemanticType_TIMESTAMP, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), schema.Fields[3])
		assert.EqualValues(t, newField("timestamp_microsecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MICROSECOND), schema.Fields[4])
		assert.EqualValues(t, newField("timestamp_nanosecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_NANOSECOND), schema.Fields[5])
	}

	type Monitor struct {
		DATE                  int64 `greptime:"tag;column:date_column;type:date"`
		DATETIME              int64 `greptime:"field;column:datetime_column;type:datetime"`
		TIMESTAMP_SECOND      int64 `greptime:"field;column:timestamp_second_column;type:timestamp;precision:second"`
		TIMESTAMP_MILLISECOND int64 `greptime:"timestamp;column:timestamp_millisecond_column;type:timestamp;precision:millisecond"`
		TIMESTAMP_MICROSECOND int64 `greptime:"field;column:timestamp_microsecond_column;type:timestamp;precision:microsecond"`
		TIMESTAMP_NANOSECOND  int64 `greptime:"field;column:timestamp_nanosecond_column;type:timestamp;precision:nanosecond"`
	}

	schema, err := ParseSchema(Monitor{})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	schema, err = ParseSchema(&Monitor{})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	schema, err = ParseSchema([]Monitor{{}})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	var monitor *Monitor
	schema, err = ParseSchema(monitor)
	assert.Nil(t, err)
	assertSchema(t, *schema)
}

type MonitorWithTableName struct {
	INT int64 `greptime:"column:int_column;type:int32"`
}

func (m *MonitorWithTableName) TableName() string {
	return "monitor_table_name_by_function"
}

func TestParseWithTableName(t *testing.T) {

	assertSchema := func(t *testing.T, schema Schema) {
		assert.Equal(t, "monitor_table_name_by_function", schema.TableName)
		assert.Len(t, schema.Fields, 1)

		assert.EqualValues(t, newField("int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), schema.Fields[0])
	}

	schema, err := ParseSchema(MonitorWithTableName{})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	schema, err = ParseSchema(&MonitorWithTableName{})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	schema, err = ParseSchema([]MonitorWithTableName{{}})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	var monitor *MonitorWithTableName
	schema, err = ParseSchema(monitor)
	assert.Nil(t, err)
	assertSchema(t, *schema)
}
