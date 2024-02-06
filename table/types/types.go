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

package types

import (
	"fmt"
	"strings"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type TimestampPrecision string

const (
	SECOND      TimestampPrecision = "second"
	MILLISECOND TimestampPrecision = "millisecond"
	MICROSECOND TimestampPrecision = "microsecond"
	NANOSECOND  TimestampPrecision = "nanosecond"
)

func (p TimestampPrecision) String() string {
	return string(p)
}

func ParseTimestampPrecision(precision string) gpb.ColumnDataType {
	switch strings.ToLower(precision) {
	case SECOND.String():
		return gpb.ColumnDataType_TIMESTAMP_SECOND
	case MILLISECOND.String():
		return gpb.ColumnDataType_TIMESTAMP_MILLISECOND
	case MICROSECOND.String():
		return gpb.ColumnDataType_TIMESTAMP_MICROSECOND
	case NANOSECOND.String():
		return gpb.ColumnDataType_TIMESTAMP_NANOSECOND
	default:
		return gpb.ColumnDataType_TIMESTAMP_MILLISECOND
	}
}

type ColumnType int

// DO NOT CHANGE THE ORDER OF THESE CONSTANTS
// THEY WILL KEEP EXACTLY THE SAME ORDER WITH PROTOCOL BUFFER
// https://github.com/GreptimeTeam/greptime-proto/blob/main/proto/greptime/v1/common.proto#L78-L110
//
// ColumnType has richer types than ColumnDataType in protocol buffer
const (
	BOOLEAN               ColumnType = 0
	INT8                  ColumnType = 1
	INT16                 ColumnType = 2
	INT32                 ColumnType = 3
	INT64                 ColumnType = 4
	UINT8                 ColumnType = 5
	UINT16                ColumnType = 6
	UINT32                ColumnType = 7
	UINT64                ColumnType = 8
	FLOAT32               ColumnType = 9
	FLOAT64               ColumnType = 10
	BINARY                ColumnType = 11
	STRING                ColumnType = 12
	DATE                  ColumnType = 13
	DATETIME              ColumnType = 14
	TIMESTAMP_SECOND      ColumnType = 15
	TIMESTAMP_MILLISECOND ColumnType = 16
	TIMESTAMP_MICROSECOND ColumnType = 17
	TIMESTAMP_NANOSECOND  ColumnType = 18
	// TIME_SECOND             ColumnType = 19
	// TIME_MILLISECOND        ColumnType = 20
	// TIME_MICROSECOND        ColumnType = 21
	// TIME_NANOSECOND         ColumnType = 22
	// INTERVAL_YEAR_MONTH     ColumnType = 23
	// INTERVAL_DAY_TIME       ColumnType = 24
	// INTERVAL_MONTH_DAY_NANO ColumnType = 25
	// DURATION_SECOND         ColumnType = 26
	// DURATION_MILLISECOND    ColumnType = 27
	// DURATION_MICROSECOND    ColumnType = 28
	// DURATION_NANOSECOND     ColumnType = 29
	// DECIMAL128              ColumnType = 30

	// the following types are not from protocol buffer
	INT       ColumnType = 101
	UINT      ColumnType = 102
	FLOAT     ColumnType = 103
	TIMESTAMP ColumnType = 104
	BYTES     ColumnType = 105 // eq BINARY
	BOOL      ColumnType = 106 // eq BOOLEAN
)

func (type_ ColumnType) String() string {
	switch type_ {
	case BOOL:
		return "BOOL"
	case BOOLEAN:
		return "BOOLEAN"
	case INT8:
		return "INT8"
	case INT16:
		return "INT16"
	case INT32:
		return "INT32"
	case INT64:
		return "INT64"
	case INT:
		return "INT"
	case UINT8:
		return "UINT8"
	case UINT16:
		return "UINT16"
	case UINT32:
		return "UINT32"
	case UINT64:
		return "UINT64"
	case UINT:
		return "UINT"
	case FLOAT32:
		return "FLOAT32"
	case FLOAT64:
		return "FLOAT64"
	case FLOAT:
		return "FLOAT"
	case BINARY:
		return "BINARY"
	case BYTES:
		return "BYTES"
	case STRING:
		return "STRING"
	case DATE:
		return "DATE"
	case DATETIME:
		return "DATETIME"
	case TIMESTAMP:
		return "TIMESTAMP"
	case TIMESTAMP_SECOND:
		return "TIMESTAMP_SECOND"
	case TIMESTAMP_MILLISECOND:
		return "TIMESTAMP_MILLISECOND"
	case TIMESTAMP_MICROSECOND:
		return "TIMESTAMP_MICROSECOND"
	case TIMESTAMP_NANOSECOND:
		return "TIMESTAMP_NANOSECOND"
	default:
		return "UNKNOWN"
	}
}

func ParseColumnType(type_, precision string) (gpb.ColumnDataType, error) {
	switch strings.ToUpper(type_) {
	case BOOLEAN.String(), BOOL.String():
		return gpb.ColumnDataType_BOOLEAN, nil
	case INT8.String():
		return gpb.ColumnDataType_INT8, nil
	case INT16.String():
		return gpb.ColumnDataType_INT16, nil
	case INT32.String():
		return gpb.ColumnDataType_INT32, nil
	case INT64.String(), INT.String():
		return gpb.ColumnDataType_INT64, nil
	case UINT8.String():
		return gpb.ColumnDataType_UINT8, nil
	case UINT16.String():
		return gpb.ColumnDataType_UINT16, nil
	case UINT32.String():
		return gpb.ColumnDataType_UINT32, nil
	case UINT64.String(), UINT.String():
		return gpb.ColumnDataType_UINT64, nil
	case FLOAT32.String():
		return gpb.ColumnDataType_FLOAT32, nil
	case FLOAT64.String(), FLOAT.String():
		return gpb.ColumnDataType_FLOAT64, nil
	case BINARY.String(), BYTES.String():
		return gpb.ColumnDataType_BINARY, nil
	case STRING.String():
		return gpb.ColumnDataType_STRING, nil
	case DATE.String():
		return gpb.ColumnDataType_DATE, nil
	case DATETIME.String():
		return gpb.ColumnDataType_DATETIME, nil
	case TIMESTAMP.String():
		return ParseTimestampPrecision(precision), nil
	case TIMESTAMP_SECOND.String():
		return gpb.ColumnDataType_TIMESTAMP_SECOND, nil
	case TIMESTAMP_MILLISECOND.String():
		return gpb.ColumnDataType_TIMESTAMP_MILLISECOND, nil
	case TIMESTAMP_MICROSECOND.String():
		return gpb.ColumnDataType_TIMESTAMP_MICROSECOND, nil
	case TIMESTAMP_NANOSECOND.String():
		return gpb.ColumnDataType_TIMESTAMP_NANOSECOND, nil
	default:
		return 0, fmt.Errorf("parse: unsupported column type %q", type_)
	}
}

func ConvertType(type_ ColumnType) (gpb.ColumnDataType, error) {
	switch type_ {
	case BOOLEAN, BOOL:
		return gpb.ColumnDataType_BOOLEAN, nil
	case INT8:
		return gpb.ColumnDataType_INT8, nil
	case INT16:
		return gpb.ColumnDataType_INT16, nil
	case INT32:
		return gpb.ColumnDataType_INT32, nil
	case INT64, INT:
		return gpb.ColumnDataType_INT64, nil
	case UINT8:
		return gpb.ColumnDataType_UINT8, nil
	case UINT16:
		return gpb.ColumnDataType_UINT16, nil
	case UINT32:
		return gpb.ColumnDataType_UINT32, nil
	case UINT64, UINT:
		return gpb.ColumnDataType_UINT64, nil
	case FLOAT32:
		return gpb.ColumnDataType_FLOAT32, nil
	case FLOAT64, FLOAT:
		return gpb.ColumnDataType_FLOAT64, nil
	case BINARY, BYTES:
		return gpb.ColumnDataType_BINARY, nil
	case STRING:
		return gpb.ColumnDataType_STRING, nil
	case DATE:
		return gpb.ColumnDataType_DATE, nil
	case DATETIME:
		return gpb.ColumnDataType_DATETIME, nil
	case TIMESTAMP_SECOND:
		return gpb.ColumnDataType_TIMESTAMP_SECOND, nil
	case TIMESTAMP_MILLISECOND, TIMESTAMP:
		return gpb.ColumnDataType_TIMESTAMP_MILLISECOND, nil
	case TIMESTAMP_MICROSECOND:
		return gpb.ColumnDataType_TIMESTAMP_MICROSECOND, nil
	case TIMESTAMP_NANOSECOND:
		return gpb.ColumnDataType_TIMESTAMP_NANOSECOND, nil
	default:
		return 0, fmt.Errorf("convert: unsupported column type %q", type_.String())
	}

}
