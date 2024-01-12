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
	"strings"
	"testing"
	"time"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"

	gerr "github.com/GreptimeTeam/greptimedb-ingester-go/error"
)

func TestConvertValue(t *testing.T) {
	// bool
	var expectBool bool = true
	val, err := Convert(expectBool)
	assert.Nil(t, err)
	assert.Equal(t, expectBool, val.Val)
	assert.Equal(t, greptime.ColumnDataType_BOOLEAN, val.Type)

	// string
	var expectString string = "string"
	val, err = Convert(expectString)
	assert.Nil(t, err)
	assert.Equal(t, expectString, val.Val)
	assert.Equal(t, greptime.ColumnDataType_STRING, val.Type)

	// bytes
	var expectBytes []byte = []byte("bytes")
	val, err = Convert(expectBytes)
	assert.Nil(t, err)
	assert.Equal(t, []byte("bytes"), val.Val)
	assert.Equal(t, greptime.ColumnDataType_BINARY, val.Type)

	// float64
	var expectFloat64 float64 = float64(64.0)
	val, err = Convert(expectFloat64)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat64, val.Val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, val.Type)

	// float32
	var expectFloat32 float32 = float32(32.0)
	val, err = Convert(expectFloat32)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat32, val.Val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT32, val.Type)

	// uint
	var originUint uint = uint(64)
	var expectUint uint64 = uint64(64)
	val, err = Convert(originUint)
	assert.Nil(t, err)
	assert.Equal(t, expectUint, val.Val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.Type)

	// uint64
	var expectUint64 uint64 = uint64(64)
	val, err = Convert(expectUint64)
	assert.Nil(t, err)
	assert.Equal(t, expectUint64, val.Val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.Type)

	// uint32
	var expectUint32 uint32 = uint32(32)
	val, err = Convert(expectUint32)
	assert.Nil(t, err)
	assert.Equal(t, expectUint32, val.Val)
	assert.Equal(t, greptime.ColumnDataType_UINT32, val.Type)

	// uint16
	var expectUint16 uint16 = uint16(16)
	val, err = Convert(expectUint16)
	assert.Nil(t, err)
	assert.Equal(t, expectUint16, val.Val)
	assert.Equal(t, greptime.ColumnDataType_UINT16, val.Type)

	// uint8
	var expectUint8 uint8 = uint8(8)
	val, err = Convert(expectUint8)
	assert.Nil(t, err)
	assert.Equal(t, expectUint8, val.Val)
	assert.Equal(t, greptime.ColumnDataType_UINT8, val.Type)

	// int
	var originInt int = int(64)
	var expectInt int64 = int64(64)
	val, err = Convert(originInt)
	assert.Nil(t, err)
	assert.Equal(t, expectInt, val.Val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.Type)

	// int64
	var expectInt64 int64 = int64(64)
	val, err = Convert(expectInt64)
	assert.Nil(t, err)
	assert.Equal(t, expectInt64, val.Val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.Type)

	// int32
	var expectInt32 int32 = int32(32)
	val, err = Convert(expectInt32)
	assert.Nil(t, err)
	assert.Equal(t, expectInt32, val.Val)
	assert.Equal(t, greptime.ColumnDataType_INT32, val.Type)

	// int16
	var expectInt16 int16 = int16(16)
	val, err = Convert(expectInt16)
	assert.Nil(t, err)
	assert.Equal(t, expectInt16, val.Val)
	assert.Equal(t, greptime.ColumnDataType_INT16, val.Type)

	// int8
	var expectInt8 int8 = int8(8)
	val, err = Convert(expectInt8)
	assert.Nil(t, err)
	assert.Equal(t, expectInt8, val.Val)
	assert.Equal(t, greptime.ColumnDataType_INT8, val.Type)

	// time.Time
	var originTime time.Time = time.UnixMilli(1677571339623)
	// var expectTime int64 = int64(1677571339623)
	val, err = Convert(originTime)
	assert.Nil(t, err)
	assert.Equal(t, originTime, val.Val)
	assert.Equal(t, greptime.ColumnDataType_DATETIME, val.Type)

	// type not supported
	_, err = Convert(time.April)
	assert.NotNil(t, err)
	_, err = Convert(map[string]any{})
	assert.NotNil(t, err)
	_, err = Convert(func() {})
	assert.NotNil(t, err)

}

func TestConvertValuePtr(t *testing.T) {
	// bool
	var expectBool bool = true
	val, err := Convert(&expectBool)
	assert.Nil(t, err)
	assert.Equal(t, expectBool, val.Val)
	assert.Equal(t, greptime.ColumnDataType_BOOLEAN, val.Type)

	// string
	var expectString string = "string"
	val, err = Convert(&expectString)
	assert.Nil(t, err)
	assert.Equal(t, expectString, val.Val)
	assert.Equal(t, greptime.ColumnDataType_STRING, val.Type)

	// bytes
	var expectBytes []byte = []byte("bytes")
	val, err = Convert(&expectBytes)
	assert.Nil(t, err)
	assert.Equal(t, []byte("bytes"), val.Val)
	assert.Equal(t, greptime.ColumnDataType_BINARY, val.Type)

	// float64
	var expectFloat64 float64 = float64(64.0)
	val, err = Convert(&expectFloat64)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat64, val.Val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, val.Type)

	// float32
	var expectFloat32 float32 = float32(32.0)
	val, err = Convert(&expectFloat32)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat32, val.Val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT32, val.Type)

	// uint
	var originUint uint = uint(64)
	var expectUint uint64 = uint64(64)
	val, err = Convert(&originUint)
	assert.Nil(t, err)
	assert.Equal(t, expectUint, val.Val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.Type)

	// uint64
	var expectUint64 uint64 = uint64(64)
	val, err = Convert(&expectUint64)
	assert.Nil(t, err)
	assert.Equal(t, expectUint64, val.Val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.Type)

	// uint32
	var expectUint32 uint32 = uint32(32)
	val, err = Convert(&expectUint32)
	assert.Nil(t, err)
	assert.Equal(t, expectUint32, val.Val)
	assert.Equal(t, greptime.ColumnDataType_UINT32, val.Type)

	// uint16
	var expectUint16 uint16 = uint16(16)
	val, err = Convert(&expectUint16)
	assert.Nil(t, err)
	assert.Equal(t, expectUint16, val.Val)
	assert.Equal(t, greptime.ColumnDataType_UINT16, val.Type)

	// uint8
	var expectUint8 uint8 = uint8(8)
	val, err = Convert(&expectUint8)
	assert.Nil(t, err)
	assert.Equal(t, expectUint8, val.Val)
	assert.Equal(t, greptime.ColumnDataType_UINT8, val.Type)

	// int
	var originInt int = int(64)
	var expectInt int64 = int64(64)
	val, err = Convert(&originInt)
	assert.Nil(t, err)
	assert.Equal(t, expectInt, val.Val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.Type)

	// int64
	var expectInt64 int64 = int64(64)
	val, err = Convert(&expectInt64)
	assert.Nil(t, err)
	assert.Equal(t, expectInt64, val.Val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.Type)

	// int32
	var expectInt32 int32 = int32(32)
	val, err = Convert(&expectInt32)
	assert.Nil(t, err)
	assert.Equal(t, expectInt32, val.Val)
	assert.Equal(t, greptime.ColumnDataType_INT32, val.Type)

	// int16
	var expectInt16 int16 = int16(16)
	val, err = Convert(&expectInt16)
	assert.Nil(t, err)
	assert.Equal(t, expectInt16, val.Val)
	assert.Equal(t, greptime.ColumnDataType_INT16, val.Type)

	// int8
	var expectInt8 int8 = int8(8)
	val, err = Convert(&expectInt8)
	assert.Nil(t, err)
	assert.Equal(t, expectInt8, val.Val)
	assert.Equal(t, greptime.ColumnDataType_INT8, val.Type)

	// time.Time
	var originTime time.Time = time.UnixMilli(1677571339623)
	// var expectTime int64 = int64(1677571339623)
	val, err = Convert(&originTime)
	assert.Nil(t, err)
	assert.Equal(t, originTime, val.Val)
	assert.Equal(t, greptime.ColumnDataType_DATETIME, val.Type)

	// type not supported
	_, err = Convert(&map[string]any{})
	assert.NotNil(t, err)
}

func TestEmptyString(t *testing.T) {
	assert.True(t, IsEmptyString(""))
	assert.True(t, IsEmptyString(" "))
	assert.True(t, IsEmptyString("  "))
	assert.True(t, IsEmptyString("\t"))
}

func TestColumnName(t *testing.T) {
	key, err := ToColumnName("ts ")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = ToColumnName(" Ts")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = ToColumnName(" TS ")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = ToColumnName("DiskUsage ")
	assert.Nil(t, err)
	assert.Equal(t, "disk_usage", key)

	key, err = ToColumnName("Disk-Usage")
	assert.Nil(t, err)
	assert.Equal(t, "disk_usage", key)

	key, err = ToColumnName("   ")
	assert.NotNil(t, err)
	assert.Equal(t, "", key)

	key, err = ToColumnName(strings.Repeat("timestamp", 20))
	assert.NotNil(t, err)
	assert.Equal(t, "", key)
}

func TestPrecisionToDataType(t *testing.T) {
	_, err := PrecisionToDataType(123)
	assert.Equal(t, gerr.ErrInvalidTimePrecision, err)
}
