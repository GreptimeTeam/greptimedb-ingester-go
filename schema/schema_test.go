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
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/cell"
	"github.com/stretchr/testify/assert"
)

func TestParseSchemaWithoutTags(t *testing.T) {

	assertFields := func(t *testing.T, schema Schema) {
		assert.Equal(t, "monitor", schema.tableName)
		assert.Len(t, schema.fields, 32)

		assert.EqualValues(t, newColumnSchema("int", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.fields[0])
		assert.EqualValues(t, newColumnSchema("int8", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT8), schema.fields[1])
		assert.EqualValues(t, newColumnSchema("int16", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT16), schema.fields[2])
		assert.EqualValues(t, newColumnSchema("int32", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), schema.fields[3])
		assert.EqualValues(t, newColumnSchema("int64", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.fields[4])
		assert.EqualValues(t, newColumnSchema("uint", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.fields[5])
		assert.EqualValues(t, newColumnSchema("uint8", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT8), schema.fields[6])
		assert.EqualValues(t, newColumnSchema("uint16", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT16), schema.fields[7])
		assert.EqualValues(t, newColumnSchema("uint32", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT32), schema.fields[8])
		assert.EqualValues(t, newColumnSchema("uint64", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.fields[9])
		assert.EqualValues(t, newColumnSchema("float32", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT32), schema.fields[10])
		assert.EqualValues(t, newColumnSchema("float64", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), schema.fields[11])
		assert.EqualValues(t, newColumnSchema("boolean", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), schema.fields[12])
		assert.EqualValues(t, newColumnSchema("binary", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), schema.fields[13])
		assert.EqualValues(t, newColumnSchema("string", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING), schema.fields[14])
		assert.EqualValues(t, newColumnSchema("date", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), schema.fields[15])

		offset := 16
		assert.EqualValues(t, newColumnSchema("ptr_int", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.fields[0+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int8", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT8), schema.fields[1+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int16", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT16), schema.fields[2+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int32", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), schema.fields[3+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int64", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.fields[4+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.fields[5+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint8", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT8), schema.fields[6+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint16", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT16), schema.fields[7+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint32", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT32), schema.fields[8+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint64", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.fields[9+offset])
		assert.EqualValues(t, newColumnSchema("ptr_float32", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT32), schema.fields[10+offset])
		assert.EqualValues(t, newColumnSchema("ptr_float64", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), schema.fields[11+offset])
		assert.EqualValues(t, newColumnSchema("ptr_boolean", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), schema.fields[12+offset])
		assert.EqualValues(t, newColumnSchema("ptr_binary", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), schema.fields[13+offset])
		assert.EqualValues(t, newColumnSchema("ptr_string", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING), schema.fields[14+offset])
		assert.EqualValues(t, newColumnSchema("ptr_date", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), schema.fields[15+offset])
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

	schema, err := parseSchema(Monitor{privateField: "private"})
	assert.Nil(t, err)
	assertFields(t, *schema)

	schema, err = parseSchema(&Monitor{privateField: "private"})
	assert.Nil(t, err)
	assertFields(t, *schema)

	schema, err = parseSchema([]Monitor{{privateField: "private"}})
	assert.Nil(t, err)
	assertFields(t, *schema)

	var monitor *Monitor
	schema, err = parseSchema(monitor)
	assert.Nil(t, err)
	assertFields(t, *schema)
}

func TestParseSchemaWithTags(t *testing.T) {

	assertFields := func(t *testing.T, schema Schema) {
		assert.Equal(t, "monitor", schema.tableName)
		assert.Len(t, schema.fields, 42)

		assert.EqualValues(t, newColumnSchema("int_column", gpb.SemanticType_TAG, gpb.ColumnDataType_INT64), schema.fields[0])
		assert.EqualValues(t, newColumnSchema("int8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT8), schema.fields[1])
		assert.EqualValues(t, newColumnSchema("int16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT16), schema.fields[2])
		assert.EqualValues(t, newColumnSchema("int32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), schema.fields[3])
		assert.EqualValues(t, newColumnSchema("int64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.fields[4])
		assert.EqualValues(t, newColumnSchema("uint_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.fields[5])
		assert.EqualValues(t, newColumnSchema("uint8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT8), schema.fields[6])
		assert.EqualValues(t, newColumnSchema("uint16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT16), schema.fields[7])
		assert.EqualValues(t, newColumnSchema("uint32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT32), schema.fields[8])
		assert.EqualValues(t, newColumnSchema("uint64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.fields[9])
		assert.EqualValues(t, newColumnSchema("float32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT32), schema.fields[10])
		assert.EqualValues(t, newColumnSchema("float64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), schema.fields[11])
		assert.EqualValues(t, newColumnSchema("boolean_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), schema.fields[12])
		assert.EqualValues(t, newColumnSchema("binary_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), schema.fields[13])
		assert.EqualValues(t, newColumnSchema("string_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING), schema.fields[14])
		assert.EqualValues(t, newColumnSchema("date_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATE), schema.fields[15])
		assert.EqualValues(t, newColumnSchema("datetime_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATETIME), schema.fields[16])
		assert.EqualValues(t, newColumnSchema("timestamp_second_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_SECOND), schema.fields[17])
		assert.EqualValues(t, newColumnSchema("timestamp_millisecond_column", gpb.SemanticType_TIMESTAMP, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), schema.fields[18])
		assert.EqualValues(t, newColumnSchema("timestamp_microsecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MICROSECOND), schema.fields[19])
		assert.EqualValues(t, newColumnSchema("timestamp_nanosecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_NANOSECOND), schema.fields[20])

		offset := 21
		assert.EqualValues(t, newColumnSchema("ptr_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.fields[0+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT8), schema.fields[1+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT16), schema.fields[2+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), schema.fields[3+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), schema.fields[4+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.fields[5+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT8), schema.fields[6+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT16), schema.fields[7+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT32), schema.fields[8+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), schema.fields[9+offset])
		assert.EqualValues(t, newColumnSchema("ptr_float32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT32), schema.fields[10+offset])
		assert.EqualValues(t, newColumnSchema("ptr_float64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), schema.fields[11+offset])
		assert.EqualValues(t, newColumnSchema("ptr_boolean_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), schema.fields[12+offset])
		assert.EqualValues(t, newColumnSchema("ptr_binary_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), schema.fields[13+offset])
		assert.EqualValues(t, newColumnSchema("ptr_string_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING), schema.fields[14+offset])
		assert.EqualValues(t, newColumnSchema("ptr_date_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATE), schema.fields[15+offset])
		assert.EqualValues(t, newColumnSchema("ptr_datetime_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATETIME), schema.fields[16+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_second_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_SECOND), schema.fields[17+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_millisecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), schema.fields[18+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_microsecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MICROSECOND), schema.fields[19+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_nanosecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_NANOSECOND), schema.fields[20+offset])
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

	schema, err := parseSchema(Monitor{privateField: "private"})
	assert.Nil(t, err)
	assertFields(t, *schema)

	schema, err = parseSchema(&Monitor{privateField: "private"})
	assert.Nil(t, err)
	assertFields(t, *schema)

	schema, err = parseSchema([]Monitor{{privateField: "private"}})
	assert.Nil(t, err)
	assertFields(t, *schema)

	var monitor *Monitor
	schema, err = parseSchema(monitor)
	assert.Nil(t, err)
	assertFields(t, *schema)
}

func TestParseTimestampViaIntType(t *testing.T) {

	assertFields := func(t *testing.T, schema Schema) {
		assert.Equal(t, "monitor", schema.tableName)
		assert.Len(t, schema.fields, 6)

		assert.EqualValues(t, newColumnSchema("date_column", gpb.SemanticType_TAG, gpb.ColumnDataType_DATE), schema.fields[0])
		assert.EqualValues(t, newColumnSchema("datetime_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATETIME), schema.fields[1])
		assert.EqualValues(t, newColumnSchema("timestamp_second_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_SECOND), schema.fields[2])
		assert.EqualValues(t, newColumnSchema("timestamp_millisecond_column", gpb.SemanticType_TIMESTAMP, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), schema.fields[3])
		assert.EqualValues(t, newColumnSchema("timestamp_microsecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MICROSECOND), schema.fields[4])
		assert.EqualValues(t, newColumnSchema("timestamp_nanosecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_NANOSECOND), schema.fields[5])
	}

	type Monitor struct {
		DATE                  int64 `greptime:"tag;column:date_column;type:date"`
		DATETIME              int64 `greptime:"field;column:datetime_column;type:datetime"`
		TIMESTAMP_SECOND      int64 `greptime:"field;column:timestamp_second_column;type:timestamp;precision:second"`
		TIMESTAMP_MILLISECOND int64 `greptime:"timestamp;column:timestamp_millisecond_column;type:timestamp;precision:millisecond"`
		TIMESTAMP_MICROSECOND int64 `greptime:"field;column:timestamp_microsecond_column;type:timestamp;precision:microsecond"`
		TIMESTAMP_NANOSECOND  int64 `greptime:"field;column:timestamp_nanosecond_column;type:timestamp;precision:nanosecond"`
	}

	schema, err := parseSchema(Monitor{})
	assert.Nil(t, err)
	assertFields(t, *schema)

	schema, err = parseSchema(&Monitor{})
	assert.Nil(t, err)
	assertFields(t, *schema)

	schema, err = parseSchema([]Monitor{{}})
	assert.Nil(t, err)
	assertFields(t, *schema)

	var monitor *Monitor
	schema, err = parseSchema(monitor)
	assert.Nil(t, err)
	assertFields(t, *schema)
}

type MonitorWithTableName struct {
	INT int64 `greptime:"column:int_column;type:int32"`
}

func (m *MonitorWithTableName) TableName() string {
	return "monitor_table_name_by_function"
}

func TestParseWithTableName(t *testing.T) {

	assertFields := func(t *testing.T, schema Schema) {
		assert.Equal(t, "monitor_table_name_by_function", schema.tableName)
		assert.Len(t, schema.fields, 1)

		assert.EqualValues(t, newColumnSchema("int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), schema.fields[0])
	}

	schema, err := parseSchema(MonitorWithTableName{})
	assert.Nil(t, err)
	assertFields(t, *schema)

	schema, err = parseSchema(&MonitorWithTableName{})
	assert.Nil(t, err)
	assertFields(t, *schema)

	schema, err = parseSchema([]MonitorWithTableName{{}})
	assert.Nil(t, err)
	assertFields(t, *schema)

	var monitor *MonitorWithTableName
	schema, err = parseSchema(monitor)
	assert.Nil(t, err)
	assertFields(t, *schema)
}

func TestParseWithValues(t *testing.T) {
	INT := 1
	INT8 := int8(2)
	INT16 := int16(3)
	INT32 := int32(4)
	INT64 := int64(5)
	UINT := uint(6)
	UINT8 := uint8(7)
	UINT16 := uint16(8)
	UINT32 := uint32(9)
	UINT64 := uint64(10)
	FLOAT32 := float32(11)
	FLOAT64 := float64(12)
	BOOLEAN := true
	BINARY := []byte{1, 2, 3}
	STRING := "string"

	TIMESTAMP := time.Now()
	DATE_INT := TIMESTAMP.Unix() / int64(cell.ONE_DAY_IN_SECONDS)
	DATETIME_INT := TIMESTAMP.UnixMilli()
	TIMESTAMP_SECOND_INT := TIMESTAMP.Unix()
	TIMESTAMP_MILLISECOND_INT := TIMESTAMP.UnixMilli()
	TIMESTAMP_MICROSECOND_INT := TIMESTAMP.UnixMicro()
	TIMESTAMP_NANOSECOND_INT := TIMESTAMP.UnixNano()

	assertSchema := func(cols []*gpb.ColumnSchema) {
		assert.Len(t, cols, 54)

		assert.EqualValues(t, newColumnSchema("int_column", gpb.SemanticType_TAG, gpb.ColumnDataType_INT64), cols[0])
		assert.EqualValues(t, newColumnSchema("int8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT8), cols[1])
		assert.EqualValues(t, newColumnSchema("int16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT16), cols[2])
		assert.EqualValues(t, newColumnSchema("int32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), cols[3])
		assert.EqualValues(t, newColumnSchema("int64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), cols[4])
		assert.EqualValues(t, newColumnSchema("uint_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), cols[5])
		assert.EqualValues(t, newColumnSchema("uint8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT8), cols[6])
		assert.EqualValues(t, newColumnSchema("uint16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT16), cols[7])
		assert.EqualValues(t, newColumnSchema("uint32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT32), cols[8])
		assert.EqualValues(t, newColumnSchema("uint64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), cols[9])
		assert.EqualValues(t, newColumnSchema("float32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT32), cols[10])
		assert.EqualValues(t, newColumnSchema("float64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), cols[11])
		assert.EqualValues(t, newColumnSchema("boolean_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), cols[12])
		assert.EqualValues(t, newColumnSchema("binary_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), cols[13])
		assert.EqualValues(t, newColumnSchema("string_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING), cols[14])
		assert.EqualValues(t, newColumnSchema("date_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATE), cols[15])
		assert.EqualValues(t, newColumnSchema("datetime_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATETIME), cols[16])
		assert.EqualValues(t, newColumnSchema("timestamp_second_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_SECOND), cols[17])
		assert.EqualValues(t, newColumnSchema("timestamp_millisecond_column", gpb.SemanticType_TIMESTAMP, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), cols[18])
		assert.EqualValues(t, newColumnSchema("timestamp_microsecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MICROSECOND), cols[19])
		assert.EqualValues(t, newColumnSchema("timestamp_nanosecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_NANOSECOND), cols[20])
		assert.EqualValues(t, newColumnSchema("date_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATE), cols[21])
		assert.EqualValues(t, newColumnSchema("datetime_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATETIME), cols[22])
		assert.EqualValues(t, newColumnSchema("timestamp_second_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_SECOND), cols[23])
		assert.EqualValues(t, newColumnSchema("timestamp_millisecond_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), cols[24])
		assert.EqualValues(t, newColumnSchema("timestamp_microsecond_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MICROSECOND), cols[25])
		assert.EqualValues(t, newColumnSchema("timestamp_nanosecond_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_NANOSECOND), cols[26])

		offset := 27
		assert.EqualValues(t, newColumnSchema("ptr_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), cols[0+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT8), cols[1+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT16), cols[2+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT32), cols[3+offset])
		assert.EqualValues(t, newColumnSchema("ptr_int64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_INT64), cols[4+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), cols[5+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint8_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT8), cols[6+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint16_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT16), cols[7+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT32), cols[8+offset])
		assert.EqualValues(t, newColumnSchema("ptr_uint64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_UINT64), cols[9+offset])
		assert.EqualValues(t, newColumnSchema("ptr_float32_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT32), cols[10+offset])
		assert.EqualValues(t, newColumnSchema("ptr_float64_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_FLOAT64), cols[11+offset])
		assert.EqualValues(t, newColumnSchema("ptr_boolean_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BOOLEAN), cols[12+offset])
		assert.EqualValues(t, newColumnSchema("ptr_binary_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_BINARY), cols[13+offset])
		assert.EqualValues(t, newColumnSchema("ptr_string_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_STRING), cols[14+offset])
		assert.EqualValues(t, newColumnSchema("ptr_date_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATE), cols[15+offset])
		assert.EqualValues(t, newColumnSchema("ptr_datetime_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATETIME), cols[16+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_second_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_SECOND), cols[17+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_millisecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), cols[18+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_microsecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MICROSECOND), cols[19+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_nanosecond_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_NANOSECOND), cols[20+offset])
		assert.EqualValues(t, newColumnSchema("ptr_date_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATE), cols[21+offset])
		assert.EqualValues(t, newColumnSchema("ptr_datetime_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_DATETIME), cols[22+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_second_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_SECOND), cols[23+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_millisecond_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MILLISECOND), cols[24+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_microsecond_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_MICROSECOND), cols[25+offset])
		assert.EqualValues(t, newColumnSchema("ptr_timestamp_nanosecond_int_column", gpb.SemanticType_FIELD, gpb.ColumnDataType_TIMESTAMP_NANOSECOND), cols[26+offset])
	}

	assertValue := func(row *gpb.Row) {
		vals := row.Values
		assert.Len(t, vals, 54)

		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_I64Value{I64Value: int64(INT)}}, vals[0])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_I8Value{I8Value: int32(INT8)}}, vals[1])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_I16Value{I16Value: int32(INT16)}}, vals[2])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_I32Value{I32Value: INT32}}, vals[3])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_I64Value{I64Value: INT64}}, vals[4])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_U64Value{U64Value: uint64(UINT)}}, vals[5])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_U8Value{U8Value: uint32(UINT8)}}, vals[6])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_U16Value{U16Value: uint32(UINT16)}}, vals[7])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_U32Value{U32Value: UINT32}}, vals[8])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_U64Value{U64Value: UINT64}}, vals[9])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_F32Value{F32Value: FLOAT32}}, vals[10])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_F64Value{F64Value: FLOAT64}}, vals[11])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_BoolValue{BoolValue: BOOLEAN}}, vals[12])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_BinaryValue{BinaryValue: BINARY}}, vals[13])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_StringValue{StringValue: STRING}}, vals[14])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_DateValue{DateValue: int32(DATE_INT)}}, vals[15])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_DatetimeValue{DatetimeValue: DATETIME_INT}}, vals[16])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampSecondValue{TimestampSecondValue: TIMESTAMP.Unix()}}, vals[17])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampMillisecondValue{TimestampMillisecondValue: TIMESTAMP.UnixMilli()}}, vals[18])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampMicrosecondValue{TimestampMicrosecondValue: TIMESTAMP.UnixMicro()}}, vals[19])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampNanosecondValue{TimestampNanosecondValue: TIMESTAMP.UnixNano()}}, vals[20])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_DateValue{DateValue: int32(DATE_INT)}}, vals[21])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_DatetimeValue{DatetimeValue: DATETIME_INT}}, vals[22])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampSecondValue{TimestampSecondValue: TIMESTAMP.Unix()}}, vals[23])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampMillisecondValue{TimestampMillisecondValue: TIMESTAMP.UnixMilli()}}, vals[24])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampMicrosecondValue{TimestampMicrosecondValue: TIMESTAMP.UnixMicro()}}, vals[25])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampNanosecondValue{TimestampNanosecondValue: TIMESTAMP.UnixNano()}}, vals[26])

		offset := 27

		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_I64Value{I64Value: int64(INT)}}, vals[0+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_I8Value{I8Value: int32(INT8)}}, vals[1+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_I16Value{I16Value: int32(INT16)}}, vals[2+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_I32Value{I32Value: INT32}}, vals[3+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_I64Value{I64Value: INT64}}, vals[4+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_U64Value{U64Value: uint64(UINT)}}, vals[5+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_U8Value{U8Value: uint32(UINT8)}}, vals[6+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_U16Value{U16Value: uint32(UINT16)}}, vals[7+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_U32Value{U32Value: UINT32}}, vals[8+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_U64Value{U64Value: UINT64}}, vals[9+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_F32Value{F32Value: FLOAT32}}, vals[10+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_F64Value{F64Value: FLOAT64}}, vals[11+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_BoolValue{BoolValue: BOOLEAN}}, vals[12+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_BinaryValue{BinaryValue: BINARY}}, vals[13+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_StringValue{StringValue: STRING}}, vals[14+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_DateValue{DateValue: int32(DATE_INT)}}, vals[15+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_DatetimeValue{DatetimeValue: DATETIME_INT}}, vals[16+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampSecondValue{TimestampSecondValue: TIMESTAMP.Unix()}}, vals[17+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampMillisecondValue{TimestampMillisecondValue: TIMESTAMP.UnixMilli()}}, vals[18+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampMicrosecondValue{TimestampMicrosecondValue: TIMESTAMP.UnixMicro()}}, vals[19+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampNanosecondValue{TimestampNanosecondValue: TIMESTAMP.UnixNano()}}, vals[20+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_DateValue{DateValue: int32(DATE_INT)}}, vals[21+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_DatetimeValue{DatetimeValue: DATETIME_INT}}, vals[22+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampSecondValue{TimestampSecondValue: TIMESTAMP.Unix()}}, vals[23+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampMillisecondValue{TimestampMillisecondValue: TIMESTAMP.UnixMilli()}}, vals[24+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampMicrosecondValue{TimestampMicrosecondValue: TIMESTAMP.UnixMicro()}}, vals[25+offset])
		assert.EqualValues(t, &gpb.Value{ValueData: &gpb.Value_TimestampNanosecondValue{TimestampNanosecondValue: TIMESTAMP.UnixNano()}}, vals[26+offset])
	}

	type Monitor struct {
		INT                       int       `greptime:"tag;column:int_column;type:int"`
		INT8                      int8      `greptime:"field;column:int8_column;type:int8"`
		INT16                     int16     `greptime:"field;column:int16_column;type:int16"`
		INT32                     int32     `greptime:"field;column:int32_column;type:int32"`
		INT64                     int64     `greptime:"field;column:int64_column;type:int64"`
		UINT                      uint      `greptime:"field;column:uint_column;type:uint"`
		UINT8                     uint8     `greptime:"field;column:uint8_column;type:uint8"`
		UINT16                    uint16    `greptime:"field;column:uint16_column;type:uint16"`
		UINT32                    uint32    `greptime:"field;column:uint32_column;type:uint32"`
		UINT64                    uint64    `greptime:"field;column:uint64_column;type:uint64"`
		FLOAT32                   float32   `greptime:"field;column:float32_column;type:float32"`
		FLOAT64                   float64   `greptime:"field;column:float64_column;type:float64"`
		BOOLEAN                   bool      `greptime:"field;column:boolean_column;type:boolean"`
		BINARY                    []byte    `greptime:"field;column:binary_column;type:binary"`
		STRING                    string    `greptime:"field;column:string_column;type:string"`
		DATE                      time.Time `greptime:"field;column:date_column;type:date"`
		DATETIME                  time.Time `greptime:"field;column:datetime_column;type:datetime"`
		TIMESTAMP_SECOND          time.Time `greptime:"field;column:timestamp_second_column;type:timestamp;precision:second"`
		TIMESTAMP_MILLISECOND     time.Time `greptime:"timestamp;column:timestamp_millisecond_column;type:timestamp;precision:millisecond"`
		TIMESTAMP_MICROSECOND     time.Time `greptime:"field;column:timestamp_microsecond_column;type:timestamp;precision:microsecond"`
		TIMESTAMP_NANOSECOND      time.Time `greptime:"field;column:timestamp_nanosecond_column;type:timestamp;precision:nanosecond"`
		DATE_INT                  int64     `greptime:"field;column:date_int_column;type:date"`
		DATETIME_INT              int64     `greptime:"field;column:datetime_int_column;type:datetime"`
		TIMESTAMP_SECOND_INT      int64     `greptime:"field;column:timestamp_second_int_column;type:timestamp;precision:second"`
		TIMESTAMP_MILLISECOND_INT int64     `greptime:"field;column:timestamp_millisecond_int_column;type:timestamp;precision:millisecond"`
		TIMESTAMP_MICROSECOND_INT int64     `greptime:"field;column:timestamp_microsecond_int_column;type:timestamp;precision:microsecond"`
		TIMESTAMP_NANOSECOND_INT  int64     `greptime:"field;column:timestamp_nanosecond_int_column;type:timestamp;precision:nanosecond"`

		PtrINT                       *int       `greptime:"field;column:ptr_int_column;type:int"`
		PtrINT8                      *int8      `greptime:"field;column:ptr_int8_column;type:int8"`
		PtrINT16                     *int16     `greptime:"field;column:ptr_int16_column;type:int16"`
		PtrINT32                     *int32     `greptime:"field;column:ptr_int32_column;type:int32"`
		PtrINT64                     *int64     `greptime:"field;column:ptr_int64_column;type:int64"`
		PtrUINT                      *uint      `greptime:"field;column:ptr_uint_column;type:uint"`
		PtrUINT8                     *uint8     `greptime:"field;column:ptr_uint8_column;type:uint8"`
		PtrUINT16                    *uint16    `greptime:"field;column:ptr_uint16_column;type:uint16"`
		PtrUINT32                    *uint32    `greptime:"field;column:ptr_uint32_column;type:uint32"`
		PtrUINT64                    *uint64    `greptime:"field;column:ptr_uint64_column;type:uint64"`
		PtrFLOAT32                   *float32   `greptime:"field;column:ptr_float32_column;type:float32"`
		PtrFLOAT64                   *float64   `greptime:"field;column:ptr_float64_column;type:float64"`
		PtrBOOLEAN                   *bool      `greptime:"field;column:ptr_boolean_column;type:boolean"`
		PtrBINARY                    *[]byte    `greptime:"field;column:ptr_binary_column;type:binary"`
		PtrSTRING                    *string    `greptime:"field;column:ptr_string_column;type:string"`
		PtrDATE                      *time.Time `greptime:"field;column:ptr_date_column;type:date"`
		PtrDATETIME                  *time.Time `greptime:"field;column:ptr_datetime_column;type:datetime"`
		PtrTIMESTAMP_SECOND          *time.Time `greptime:"field;column:ptr_timestamp_second_column;type:timestamp;precision:second"`
		PtrTIMESTAMP_MILLISECOND     *time.Time `greptime:"field;column:ptr_timestamp_millisecond_column;type:timestamp;precision:millisecond"`
		PtrTIMESTAMP_MICROSECOND     *time.Time `greptime:"field;column:ptr_timestamp_microsecond_column;type:timestamp;precision:microsecond"`
		PtrTIMESTAMP_NANOSECOND      *time.Time `greptime:"field;column:ptr_timestamp_nanosecond_column;type:timestamp;precision:nanosecond"`
		PtrDATE_INT                  *int64     `greptime:"field;column:ptr_date_int_column;type:date"`
		PtrDATETIME_INT              *int64     `greptime:"field;column:ptr_datetime_int_column;type:datetime"`
		PtrTIMESTAMP_SECOND_INT      *int64     `greptime:"field;column:ptr_timestamp_second_int_column;type:timestamp;precision:second"`
		PtrTIMESTAMP_MILLISECOND_INT *int64     `greptime:"field;column:ptr_timestamp_millisecond_int_column;type:timestamp;precision:millisecond"`
		PtrTIMESTAMP_MICROSECOND_INT *int64     `greptime:"field;column:ptr_timestamp_microsecond_int_column;type:timestamp;precision:microsecond"`
		PtrTIMESTAMP_NANOSECOND_INT  *int64     `greptime:"field;column:ptr_timestamp_nanosecond_int_column;type:timestamp;precision:nanosecond"`

		privateField string // will be ignored
	}

	monitor := Monitor{
		INT:                       INT,
		INT8:                      INT8,
		INT16:                     INT16,
		INT32:                     INT32,
		INT64:                     INT64,
		UINT:                      UINT,
		UINT8:                     UINT8,
		UINT16:                    UINT16,
		UINT32:                    UINT32,
		UINT64:                    UINT64,
		FLOAT32:                   FLOAT32,
		FLOAT64:                   FLOAT64,
		BOOLEAN:                   BOOLEAN,
		BINARY:                    BINARY,
		STRING:                    STRING,
		DATE:                      TIMESTAMP,
		DATETIME:                  TIMESTAMP,
		TIMESTAMP_SECOND:          TIMESTAMP,
		TIMESTAMP_MILLISECOND:     TIMESTAMP,
		TIMESTAMP_MICROSECOND:     TIMESTAMP,
		TIMESTAMP_NANOSECOND:      TIMESTAMP,
		DATE_INT:                  DATE_INT,
		DATETIME_INT:              DATETIME_INT,
		TIMESTAMP_SECOND_INT:      TIMESTAMP_SECOND_INT,
		TIMESTAMP_MILLISECOND_INT: TIMESTAMP_MILLISECOND_INT,
		TIMESTAMP_MICROSECOND_INT: TIMESTAMP_MICROSECOND_INT,
		TIMESTAMP_NANOSECOND_INT:  TIMESTAMP_NANOSECOND_INT,

		PtrINT:                       &INT,
		PtrINT8:                      &INT8,
		PtrINT16:                     &INT16,
		PtrINT32:                     &INT32,
		PtrINT64:                     &INT64,
		PtrUINT:                      &UINT,
		PtrUINT8:                     &UINT8,
		PtrUINT16:                    &UINT16,
		PtrUINT32:                    &UINT32,
		PtrUINT64:                    &UINT64,
		PtrFLOAT32:                   &FLOAT32,
		PtrFLOAT64:                   &FLOAT64,
		PtrBOOLEAN:                   &BOOLEAN,
		PtrBINARY:                    &BINARY,
		PtrSTRING:                    &STRING,
		PtrDATE:                      &TIMESTAMP,
		PtrDATETIME:                  &TIMESTAMP,
		PtrTIMESTAMP_SECOND:          &TIMESTAMP,
		PtrTIMESTAMP_MILLISECOND:     &TIMESTAMP,
		PtrTIMESTAMP_MICROSECOND:     &TIMESTAMP,
		PtrTIMESTAMP_NANOSECOND:      &TIMESTAMP,
		PtrDATE_INT:                  &DATE_INT,
		PtrDATETIME_INT:              &DATETIME_INT,
		PtrTIMESTAMP_SECOND_INT:      &TIMESTAMP_SECOND_INT,
		PtrTIMESTAMP_MILLISECOND_INT: &TIMESTAMP_MILLISECOND_INT,
		PtrTIMESTAMP_MICROSECOND_INT: &TIMESTAMP_MICROSECOND_INT,
		PtrTIMESTAMP_NANOSECOND_INT:  &TIMESTAMP_NANOSECOND_INT,

		privateField: "private",
	}

	tbl, err := Parse(monitor)
	assert.Nil(t, err)
	assert.NotNil(t, tbl)

	rows := tbl.GetRows()
	assert.NotNil(t, rows)

	assertSchema(rows.Schema)
	for _, row := range rows.Rows {
		assertValue(row)
	}
}
