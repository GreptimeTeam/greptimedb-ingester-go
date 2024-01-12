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

package util

import (
	"fmt"
	"strings"
	"time"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stoewer/go-strcase"

	gerr "github.com/GreptimeTeam/greptimedb-ingester-go/error"
)

type Value struct {
	Val  any
	Type greptimepb.ColumnDataType
}

func newValue(val any, typ greptimepb.ColumnDataType) *Value {
	return &Value{val, typ}
}

func Convert(v any) (*Value, error) {
	switch t := v.(type) {
	case bool:
		return newValue(t, greptimepb.ColumnDataType_BOOLEAN), nil
	case string:
		return newValue(t, greptimepb.ColumnDataType_STRING), nil
	case []byte:
		return newValue(t, greptimepb.ColumnDataType_BINARY), nil
	case float64:
		return newValue(t, greptimepb.ColumnDataType_FLOAT64), nil
	case float32:
		return newValue(t, greptimepb.ColumnDataType_FLOAT32), nil
	case uint:
		return newValue(uint64(t), greptimepb.ColumnDataType_UINT64), nil
	case uint64:
		return newValue(t, greptimepb.ColumnDataType_UINT64), nil
	case uint32:
		return newValue(t, greptimepb.ColumnDataType_UINT32), nil
	case uint16:
		return newValue(t, greptimepb.ColumnDataType_UINT16), nil
	case uint8:
		return newValue(t, greptimepb.ColumnDataType_UINT8), nil
	case int:
		return newValue(int64(t), greptimepb.ColumnDataType_INT64), nil
	case int64:
		return newValue(t, greptimepb.ColumnDataType_INT64), nil
	case int32:
		return newValue(t, greptimepb.ColumnDataType_INT32), nil
	case int16:
		return newValue(t, greptimepb.ColumnDataType_INT16), nil
	case int8:
		return newValue(t, greptimepb.ColumnDataType_INT8), nil
	case time.Time:
		return newValue(t, greptimepb.ColumnDataType_DATETIME), nil

	case *bool:
		return newValue(*t, greptimepb.ColumnDataType_BOOLEAN), nil
	case *string:
		return newValue(*t, greptimepb.ColumnDataType_STRING), nil
	case *[]byte:
		return newValue(*t, greptimepb.ColumnDataType_BINARY), nil
	case *float64:
		return newValue(*t, greptimepb.ColumnDataType_FLOAT64), nil
	case *float32:
		return newValue(*t, greptimepb.ColumnDataType_FLOAT32), nil
	case *uint:
		return newValue(uint64(*t), greptimepb.ColumnDataType_UINT64), nil
	case *uint64:
		return newValue(*t, greptimepb.ColumnDataType_UINT64), nil
	case *uint32:
		return newValue(*t, greptimepb.ColumnDataType_UINT32), nil
	case *uint16:
		return newValue(*t, greptimepb.ColumnDataType_UINT16), nil
	case *uint8:
		return newValue(*t, greptimepb.ColumnDataType_UINT8), nil
	case *int:
		return newValue(int64(*t), greptimepb.ColumnDataType_INT64), nil
	case *int64:
		return newValue(*t, greptimepb.ColumnDataType_INT64), nil
	case *int32:
		return newValue(*t, greptimepb.ColumnDataType_INT32), nil
	case *int16:
		return newValue(*t, greptimepb.ColumnDataType_INT16), nil
	case *int8:
		return newValue(*t, greptimepb.ColumnDataType_INT8), nil
	case *time.Time:
		return newValue(*t, greptimepb.ColumnDataType_DATETIME), nil
	default:
		return nil, fmt.Errorf("the type '%T' is not supported", t)
	}
}

func IsValidPrecision(t time.Duration) bool {
	return t == time.Second ||
		t == time.Millisecond ||
		t == time.Microsecond ||
		t == time.Nanosecond
}

func PrecisionToDataType(d time.Duration) (greptimepb.ColumnDataType, error) {
	// if the precision has not been set, use default precision `time.Millisecond`
	if d == 0 {
		d = time.Millisecond
	}
	switch d {
	case time.Second:
		return greptimepb.ColumnDataType_TIMESTAMP_SECOND, nil
	case time.Millisecond:
		return greptimepb.ColumnDataType_TIMESTAMP_MILLISECOND, nil
	case time.Microsecond:
		return greptimepb.ColumnDataType_TIMESTAMP_MICROSECOND, nil
	case time.Nanosecond:
		return greptimepb.ColumnDataType_TIMESTAMP_NANOSECOND, nil
	default:
		return 0, gerr.ErrInvalidTimePrecision
	}
}

func IsEmptyString(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

func ToColumnName(s string) (string, error) {
	s = strings.TrimSpace(s)

	if len(s) == 0 {
		return "", gerr.ErrEmptyKey
	}

	if len(s) >= 100 {
		return "", fmt.Errorf("the length of column name CAN NOT be longer than 100. %s", s)
	}

	return strings.ToLower(strcase.SnakeCase(s)), nil
}
