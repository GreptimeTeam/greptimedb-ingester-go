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
		return "", fmt.Errorf("the length of column name CAN NOT be longer than 100. %q", s)
	}

	return strings.ToLower(strcase.SnakeCase(s)), nil
}
