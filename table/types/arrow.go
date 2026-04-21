/*
 * Copyright 2025 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package types

import (
	"errors"
	"fmt"

	gpbv1 "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// Timezone-less timestamp types. arrow-go's FixedWidthTypes.Timestamp_* default
// to TimeZone="UTC", but GreptimeDB's server-side schema expects Timestamp(unit)
// without a timezone for bulk (Arrow Flight) writes, and rejects the record
// batch with "expected Timestamp(ms) but found Timestamp(ms, \"UTC\")".
var (
	timestampTypeS  = &arrow.TimestampType{Unit: arrow.Second}
	timestampTypeMs = &arrow.TimestampType{Unit: arrow.Millisecond}
	timestampTypeUs = &arrow.TimestampType{Unit: arrow.Microsecond}
	timestampTypeNs = &arrow.TimestampType{Unit: arrow.Nanosecond}
)

// ArrowConverter handles conversion between GreptimeDB and Arrow formats
type ArrowConverter struct{}

func NewArrowConverter() *ArrowConverter {
	return &ArrowConverter{}
}

// ToArrow converts GreptimeDB Rows to an Arrow Record for efficient bulk transfer.
// It validates the input schema and data before performing the conversion.
func (c *ArrowConverter) ToArrow(rows *gpbv1.Rows) (arrow.Record, error) {
	// Validate input
	if rows == nil || len(rows.Rows) == 0 {
		return nil, errors.New("no rows provided")
	}
	if rows == nil || len(rows.Schema) == 0 {
		return nil, errors.New("no schema provided")
	}

	allocator := memory.NewGoAllocator()

	fields := make([]arrow.Field, len(rows.Schema))
	for i, col := range rows.Schema {
		dt, err := c.convertToArrowType(col.Datatype)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s: %w", col.ColumnName, err)
		}
		fields[i] = arrow.Field{
			Name:     col.ColumnName,
			Type:     dt,
			Nullable: true,
		}
	}
	schema := arrow.NewSchema(fields, nil)

	builders := make([]array.Builder, len(rows.Schema))
	defer func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}()

	for i := range builders {
		builders[i] = array.NewBuilder(allocator, schema.Field(i).Type)
		if builders[i] == nil {
			return nil, fmt.Errorf("failed to create builder for column %s", fields[i].Name)
		}
	}

	for rowIdx, row := range rows.Rows {
		if len(row.Values) != len(rows.Schema) {
			return nil, fmt.Errorf("row %d has %d values, expected %d",
				rowIdx, len(row.Values), len(rows.Schema))
		}

		for colIdx, value := range row.Values {
			if err := c.fillValue(builders[colIdx], value, rows.Schema[colIdx].Datatype); err != nil {
				return nil, fmt.Errorf("row %d column %s: %w",
					rowIdx, rows.Schema[colIdx].ColumnName, err)
			}
		}
	}

	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		if arrays[i] == nil {
			return nil, fmt.Errorf("failed to create array for column %s", fields[i].Name)
		}
	}

	return array.NewRecord(schema, arrays, int64(len(rows.Rows))), nil
}

// convertToArrowType maps GreptimeDB column types to their corresponding Arrow types.
func (c *ArrowConverter) convertToArrowType(dt gpbv1.ColumnDataType) (arrow.DataType, error) {
	switch dt {
	case gpbv1.ColumnDataType_INT8:
		return arrow.PrimitiveTypes.Int8, nil
	case gpbv1.ColumnDataType_INT16:
		return arrow.PrimitiveTypes.Int16, nil
	case gpbv1.ColumnDataType_INT32:
		return arrow.PrimitiveTypes.Int32, nil
	case gpbv1.ColumnDataType_INT64:
		return arrow.PrimitiveTypes.Int64, nil
	case gpbv1.ColumnDataType_UINT8:
		return arrow.PrimitiveTypes.Uint8, nil
	case gpbv1.ColumnDataType_UINT16:
		return arrow.PrimitiveTypes.Uint16, nil
	case gpbv1.ColumnDataType_UINT32:
		return arrow.PrimitiveTypes.Uint32, nil
	case gpbv1.ColumnDataType_UINT64:
		return arrow.PrimitiveTypes.Uint64, nil
	case gpbv1.ColumnDataType_FLOAT32:
		return arrow.PrimitiveTypes.Float32, nil
	case gpbv1.ColumnDataType_FLOAT64:
		return arrow.PrimitiveTypes.Float64, nil
	case gpbv1.ColumnDataType_BOOLEAN:
		return arrow.FixedWidthTypes.Boolean, nil
	case gpbv1.ColumnDataType_STRING:
		return arrow.BinaryTypes.String, nil
	case gpbv1.ColumnDataType_BINARY:
		return arrow.BinaryTypes.Binary, nil
	case gpbv1.ColumnDataType_TIMESTAMP_MILLISECOND:
		return timestampTypeMs, nil
	case gpbv1.ColumnDataType_TIMESTAMP_MICROSECOND, gpbv1.ColumnDataType_DATETIME:
		// DATETIME is a deprecated alias for TIMESTAMP_MICROSECOND (greptimedb PR #5506).
		return timestampTypeUs, nil
	case gpbv1.ColumnDataType_TIMESTAMP_NANOSECOND:
		return timestampTypeNs, nil
	case gpbv1.ColumnDataType_TIMESTAMP_SECOND:
		return timestampTypeS, nil
	case gpbv1.ColumnDataType_DATE:
		return arrow.FixedWidthTypes.Date32, nil
	case gpbv1.ColumnDataType_TIME_SECOND:
		return arrow.FixedWidthTypes.Time32s, nil
	case gpbv1.ColumnDataType_TIME_MILLISECOND:
		return arrow.FixedWidthTypes.Time32ms, nil
	case gpbv1.ColumnDataType_TIME_MICROSECOND:
		return arrow.FixedWidthTypes.Time64us, nil
	case gpbv1.ColumnDataType_TIME_NANOSECOND:
		return arrow.FixedWidthTypes.Time64ns, nil
	default:
		return nil, errors.New("unsupported data type")
	}
}

// fillValue populates an Arrow builder with data from a GreptimeDB value.
// It handles null values and type-specific conversions appropriately.
func (c *ArrowConverter) fillValue(builder array.Builder, value *gpbv1.Value, dataType gpbv1.ColumnDataType) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.Int8Builder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(int8(value.GetI8Value()))
		}

	case *array.Int16Builder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(int16(value.GetI16Value()))
		}

	case *array.Int32Builder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(value.GetI32Value())
		}

	case *array.Int64Builder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(value.GetI64Value())
		}

	case *array.Uint8Builder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(uint8(value.GetU8Value()))
		}

	case *array.Uint16Builder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(uint16(value.GetU16Value()))
		}

	case *array.Uint32Builder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(value.GetU32Value())
		}

	case *array.Uint64Builder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(value.GetU64Value())
		}

	case *array.Float32Builder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(value.GetF32Value())
		}

	case *array.Float64Builder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(value.GetF64Value())
		}

	case *array.BooleanBuilder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(value.GetBoolValue())
		}

	case *array.StringBuilder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(value.GetStringValue())
		}

	case *array.BinaryBuilder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(value.GetBinaryValue())
		}

	case *array.TimestampBuilder:
		if value == nil {
			b.AppendNull()
		} else {
			var tsValue int64
			switch dataType {
			case gpbv1.ColumnDataType_TIMESTAMP_SECOND:
				tsValue = value.GetTimestampSecondValue()
			case gpbv1.ColumnDataType_TIMESTAMP_MILLISECOND:
				tsValue = value.GetTimestampMillisecondValue()
			case gpbv1.ColumnDataType_TIMESTAMP_MICROSECOND, gpbv1.ColumnDataType_DATETIME:
				tsValue = value.GetTimestampMicrosecondValue()
			case gpbv1.ColumnDataType_TIMESTAMP_NANOSECOND:
				tsValue = value.GetTimestampNanosecondValue()
			default:
				return errors.New("unsupported timestamp type")
			}
			b.Append(arrow.Timestamp(tsValue))
		}

	case *array.Date32Builder:
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(arrow.Date32(value.GetDateValue()))
		}

	default:
		return errors.New("unsupported builder type")
	}

	return nil
}
