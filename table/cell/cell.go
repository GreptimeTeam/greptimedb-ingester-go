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

package cell

import (
	"fmt"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type Cell struct {
	Val      any
	DataType gpb.ColumnDataType
}

func New(v any, dataType gpb.ColumnDataType) Cell {
	return Cell{Val: v, DataType: dataType}
}

func (c Cell) Build() (*gpb.Value, error) {
	if c.Val == nil {
		return &gpb.Value{}, nil
	}

	switch c.DataType {

	case gpb.ColumnDataType_INT8, gpb.ColumnDataType_INT16, gpb.ColumnDataType_INT32, gpb.ColumnDataType_INT64:
		return BuildInt(c.Val, c.DataType)

	case gpb.ColumnDataType_UINT8, gpb.ColumnDataType_UINT16, gpb.ColumnDataType_UINT32, gpb.ColumnDataType_UINT64:
		return BuildUint(c.Val, c.DataType)

	case gpb.ColumnDataType_FLOAT32, gpb.ColumnDataType_FLOAT64:
		return BuildFloat(c.Val, c.DataType)

	case gpb.ColumnDataType_BOOLEAN:
		return BuildBool(c.Val)

	case gpb.ColumnDataType_BINARY:
		return BuildBytes(c.Val)

	case gpb.ColumnDataType_STRING:
		return BuildString(c.Val)

	case gpb.ColumnDataType_DATE:
		return BuildDate(c.Val)
	case gpb.ColumnDataType_DATETIME:
		return BuildDateTime(c.Val)

	case gpb.ColumnDataType_TIMESTAMP_SECOND:
		return BuildTimestampSecond(c.Val)
	case gpb.ColumnDataType_TIMESTAMP_MILLISECOND:
		return BuildTimestampMillisecond(c.Val)
	case gpb.ColumnDataType_TIMESTAMP_MICROSECOND:
		return BuildTimestampMicrosecond(c.Val)
	case gpb.ColumnDataType_TIMESTAMP_NANOSECOND:
		return BuildTimestampNanosecond(c.Val)

	case gpb.ColumnDataType_TIME_SECOND:
		return BuildTimeSecond(c.Val)
	case gpb.ColumnDataType_TIME_MILLISECOND:
		return BuildTimeMillisecond(c.Val)
	case gpb.ColumnDataType_TIME_MICROSECOND:
		return BuildTimeMicrosecond(c.Val)
	case gpb.ColumnDataType_TIME_NANOSECOND:
		return BuildTimeNanosecond(c.Val)

	case gpb.ColumnDataType_INTERVAL_YEAR_MONTH,
		gpb.ColumnDataType_INTERVAL_DAY_TIME,
		gpb.ColumnDataType_INTERVAL_MONTH_DAY_NANO:
		return nil, fmt.Errorf("INTERVAL not implemented yet for %#v", c.Val)

	case gpb.ColumnDataType_DURATION_SECOND,
		gpb.ColumnDataType_DURATION_MILLISECOND,
		gpb.ColumnDataType_DURATION_MICROSECOND,
		gpb.ColumnDataType_DURATION_NANOSECOND:
		return nil, fmt.Errorf("DURATION not supported for %#v", c.Val)

	// TODO(yuanbohan): support decimal 128
	case gpb.ColumnDataType_DECIMAL128:
		return nil, fmt.Errorf("DECIMAL 128 not supported for %#v", c.Val)
	default:
		return nil, fmt.Errorf("unknown column data type: %v", c.DataType)
	}
}
