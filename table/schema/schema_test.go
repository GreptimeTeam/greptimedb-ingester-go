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

	"github.com/stretchr/testify/assert"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

func assertSchema(t *testing.T, schema Schema) {
	assert.Equal(t, "monitor", schema.Name)
	assert.Len(t, schema.Fields, 32)

	assert.EqualValues(t, Field{Name: "int", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_INT64}, schema.Fields[0])
	assert.EqualValues(t, Field{Name: "int8", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_INT8}, schema.Fields[1])
	assert.EqualValues(t, Field{Name: "int16", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_INT16}, schema.Fields[2])
	assert.EqualValues(t, Field{Name: "int32", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_INT32}, schema.Fields[3])
	assert.EqualValues(t, Field{Name: "int64", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_INT64}, schema.Fields[4])
	assert.EqualValues(t, Field{Name: "uint", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_UINT64}, schema.Fields[5])
	assert.EqualValues(t, Field{Name: "uint8", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_UINT8}, schema.Fields[6])
	assert.EqualValues(t, Field{Name: "uint16", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_UINT16}, schema.Fields[7])
	assert.EqualValues(t, Field{Name: "uint32", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_UINT32}, schema.Fields[8])
	assert.EqualValues(t, Field{Name: "uint64", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_UINT64}, schema.Fields[9])
	assert.EqualValues(t, Field{Name: "float32", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT32}, schema.Fields[10])
	assert.EqualValues(t, Field{Name: "float64", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64}, schema.Fields[11])
	assert.EqualValues(t, Field{Name: "boolean", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_BOOLEAN}, schema.Fields[12])
	assert.EqualValues(t, Field{Name: "binary", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_BINARY}, schema.Fields[13])
	assert.EqualValues(t, Field{Name: "string", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_STRING}, schema.Fields[14])
	assert.EqualValues(t, Field{Name: "date", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_TIMESTAMP_MILLISECOND}, schema.Fields[15])

	offset := 16
	assert.EqualValues(t, Field{Name: "ptr_int", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_INT64}, schema.Fields[0+offset])
	assert.EqualValues(t, Field{Name: "ptr_int8", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_INT8}, schema.Fields[1+offset])
	assert.EqualValues(t, Field{Name: "ptr_int16", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_INT16}, schema.Fields[2+offset])
	assert.EqualValues(t, Field{Name: "ptr_int32", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_INT32}, schema.Fields[3+offset])
	assert.EqualValues(t, Field{Name: "ptr_int64", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_INT64}, schema.Fields[4+offset])
	assert.EqualValues(t, Field{Name: "ptr_uint", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_UINT64}, schema.Fields[5+offset])
	assert.EqualValues(t, Field{Name: "ptr_uint8", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_UINT8}, schema.Fields[6+offset])
	assert.EqualValues(t, Field{Name: "ptr_uint16", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_UINT16}, schema.Fields[7+offset])
	assert.EqualValues(t, Field{Name: "ptr_uint32", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_UINT32}, schema.Fields[8+offset])
	assert.EqualValues(t, Field{Name: "ptr_uint64", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_UINT64}, schema.Fields[9+offset])
	assert.EqualValues(t, Field{Name: "ptr_float32", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT32}, schema.Fields[10+offset])
	assert.EqualValues(t, Field{Name: "ptr_float64", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64}, schema.Fields[11+offset])
	assert.EqualValues(t, Field{Name: "ptr_boolean", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_BOOLEAN}, schema.Fields[12+offset])
	assert.EqualValues(t, Field{Name: "ptr_binary", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_BINARY}, schema.Fields[13+offset])
	assert.EqualValues(t, Field{Name: "ptr_string", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_STRING}, schema.Fields[14+offset])
	assert.EqualValues(t, Field{Name: "ptr_date", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_TIMESTAMP_MILLISECOND}, schema.Fields[15+offset])

}

func TestParseSchemaWithoutTags(t *testing.T) {

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
	assertSchema(t, *schema)

	schema, err = parseSchema(&Monitor{privateField: "private"})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	schema, err = parseSchema([]Monitor{{privateField: "private"}})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	var monitor *Monitor
	schema, err = parseSchema(monitor)
	assert.Nil(t, err)
	assertSchema(t, *schema)
}

func TestParseSchemaWithTags(t *testing.T) {

	type Monitor struct {
		INT     int `greptime:"tag;column=int_column;type=int"`
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
	assertSchema(t, *schema)

	schema, err = parseSchema(&Monitor{privateField: "private"})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	schema, err = parseSchema([]Monitor{{privateField: "private"}})
	assert.Nil(t, err)
	assertSchema(t, *schema)

	var monitor *Monitor
	schema, err = parseSchema(monitor)
	assert.Nil(t, err)
	assertSchema(t, *schema)
}
