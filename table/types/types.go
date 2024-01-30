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

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type ColumnType int

// DO NOT CHANGE THE ORDER OF THESE CONSTANTS
// THEY WILL KEEP EXACTLY THE SAME ORDER WITH PROTOCOL BUFFER
// https://github.com/GreptimeTeam/greptime-proto/blob/main/proto/greptime/v1/common.proto#L78-L110
const (
	BOOLEAN                 ColumnType = 0
	INT8                    ColumnType = 1
	INT16                   ColumnType = 2
	INT32                   ColumnType = 3
	INT64                   ColumnType = 4
	UINT8                   ColumnType = 5
	UINT16                  ColumnType = 6
	UINT32                  ColumnType = 7
	UINT64                  ColumnType = 8
	FLOAT32                 ColumnType = 9
	FLOAT64                 ColumnType = 10
	BINARY                  ColumnType = 11
	STRING                  ColumnType = 12
	DATE                    ColumnType = 13
	DATETIME                ColumnType = 14
	TIMESTAMP_SECOND        ColumnType = 15
	TIMESTAMP_MILLISECOND   ColumnType = 16
	TIMESTAMP_MICROSECOND   ColumnType = 17
	TIMESTAMP_NANOSECOND    ColumnType = 18
	TIME_SECOND             ColumnType = 19
	TIME_MILLISECOND        ColumnType = 20
	TIME_MICROSECOND        ColumnType = 21
	TIME_NANOSECOND         ColumnType = 22
	INTERVAL_YEAR_MONTH     ColumnType = 23
	INTERVAL_DAY_TIME       ColumnType = 24
	INTERVAL_MONTH_DAY_NANO ColumnType = 25
	DURATION_SECOND         ColumnType = 26
	DURATION_MILLISECOND    ColumnType = 27
	DURATION_MICROSECOND    ColumnType = 28
	DURATION_NANOSECOND     ColumnType = 29
	DECIMAL128              ColumnType = 30
)

func GetColumnType(type_ ColumnType) (gpb.ColumnDataType, error) {
	switch type_ {
	case BOOLEAN:
		return gpb.ColumnDataType_BOOLEAN, nil
	case INT8:
		return gpb.ColumnDataType_INT8, nil
	case INT16:
		return gpb.ColumnDataType_INT16, nil
	case INT32:
		return gpb.ColumnDataType_INT32, nil
	case INT64:
		return gpb.ColumnDataType_INT64, nil
	case UINT8:
		return gpb.ColumnDataType_UINT8, nil
	case UINT16:
		return gpb.ColumnDataType_UINT16, nil
	case UINT32:
		return gpb.ColumnDataType_UINT32, nil
	case UINT64:
		return gpb.ColumnDataType_UINT64, nil
	case FLOAT32:
		return gpb.ColumnDataType_FLOAT32, nil
	case FLOAT64:
		return gpb.ColumnDataType_FLOAT64, nil
	case BINARY:
		return gpb.ColumnDataType_BINARY, nil
	case STRING:
		return gpb.ColumnDataType_STRING, nil
	case DATE:
		return gpb.ColumnDataType_DATE, nil
	case DATETIME:
		return gpb.ColumnDataType_DATETIME, nil
	case TIMESTAMP_SECOND:
		return gpb.ColumnDataType_TIMESTAMP_SECOND, nil
	case TIMESTAMP_MILLISECOND:
		return gpb.ColumnDataType_TIMESTAMP_MILLISECOND, nil
	case TIMESTAMP_MICROSECOND:
		return gpb.ColumnDataType_TIMESTAMP_MICROSECOND, nil
	case TIMESTAMP_NANOSECOND:
		return gpb.ColumnDataType_TIMESTAMP_NANOSECOND, nil
	case TIME_SECOND:
		return gpb.ColumnDataType_TIME_SECOND, nil
	case TIME_MILLISECOND:
		return gpb.ColumnDataType_TIME_MILLISECOND, nil
	case TIME_MICROSECOND:
		return gpb.ColumnDataType_TIME_MICROSECOND, nil
	case TIME_NANOSECOND:
		return gpb.ColumnDataType_TIME_NANOSECOND, nil
	case INTERVAL_YEAR_MONTH:
		return gpb.ColumnDataType_INTERVAL_YEAR_MONTH, nil
	case INTERVAL_DAY_TIME:
		return gpb.ColumnDataType_INTERVAL_DAY_TIME, nil
	case INTERVAL_MONTH_DAY_NANO:
		return gpb.ColumnDataType_INTERVAL_MONTH_DAY_NANO, nil
	case DURATION_SECOND:
		return gpb.ColumnDataType_DURATION_SECOND, nil
	case DURATION_MILLISECOND:
		return gpb.ColumnDataType_DURATION_MILLISECOND, nil
	case DURATION_MICROSECOND:
		return gpb.ColumnDataType_DURATION_MICROSECOND, nil
	case DURATION_NANOSECOND:
		return gpb.ColumnDataType_DURATION_NANOSECOND, nil
	case DECIMAL128:
		return gpb.ColumnDataType_DECIMAL128, nil
	default:
		return 0, fmt.Errorf("unknown column type %d", type_)
	}

}
