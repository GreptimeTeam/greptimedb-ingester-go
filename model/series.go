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

package model

import (
	"fmt"
	"time"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	gutil "github.com/GreptimeTeam/greptimedb-ingester-go/util"
)

type column struct {
	typ      greptimepb.ColumnDataType
	semantic greptimepb.SemanticType
}

func checkColumnEquality(key string, col1, col2 column) error {
	if col1.typ != col2.typ {
		return fmt.Errorf("the type of '%s' does not match: '%v' and '%v'", key, col1.typ, col2.typ)
	}
	if col1.semantic != col2.semantic {
		return fmt.Errorf("tag and field MUST NOT contain same key: %q", key)
	}

	return nil
}

// Series represents one row of data you want to insert into GreptimeDB.
//   - Tag fields are the index columns, which helps you to query data efficiently
//   - Field fields are the value columns, which are used for value
//   - Timestamp field is the timestamp column, which is required
//
// you do not need to create schema in advance, it will be created based on Series.
// But once the schema is created, [Client] has no ability to alert it.
type Series struct {
	orders  []string
	columns map[string]column
	vals    map[string]any

	timestamp time.Time // required for inserting
}

// GetTagsAndFields get all column names from metric, except timestamp column
func (s *Series) GetTagsAndFields() []string {
	dst := make([]string, len(s.orders))
	copy(dst, s.orders)
	return dst
}

// Get helps to get value of specified column. The second return value
// indicates if the key was present in Series
func (s *Series) Get(key string) (any, bool) {
	val, exist := s.vals[key]
	return val, exist
}

// GetUint helps to get uint64 type of the specified key. It can retrieve the following type:
//   - uint64
//   - uint32
//   - uint16
//   - uint8
//   - uint
//
// if you want uint32 instead of uint64, you can do it like:
//
//	if v, ok := s.GetUint(key); ok {
//		val := uint32(v)
//	}
func (s *Series) GetUint(key string) (uint64, bool) {
	val, exist := s.Get(key)
	if !exist {
		return 0, exist
	}

	switch t := val.(type) {
	case uint8:
		return uint64(t), true
	case uint16:
		return uint64(t), true
	case uint32:
		return uint64(t), true
	case uint64:
		return t, true
	case uint:
		return uint64(t), true
	default:
		return 0, false
	}
}

// GetInt helps to get int64 type of the specified key. It can retrieve the following type:
//   - int64
//   - int32
//   - int16
//   - int8
//   - int
//
// if you want int32 instead of int64, you can do it like:
//
//	if v, ok := s.GetInt(key); ok {
//		val := int32(v)
//	}
func (s *Series) GetInt(key string) (int64, bool) {
	val, exist := s.Get(key)
	if !exist {
		return 0, exist
	}

	switch t := val.(type) {
	case int8:
		return int64(t), true
	case int16:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	default:
		return 0, false
	}
}

// GetFloat helps to get float64 type of the specified key. It can retrieve the following type:
//   - float64
//   - float32
//
// if you want float32 instead of float64, you can do it like:
//
//	if v, ok := s.GetFloat(key); ok {
//		val := float32(v)
//	}
func (s *Series) GetFloat(key string) (float64, bool) {
	val, exist := s.Get(key)
	if !exist {
		return 0, exist
	}

	switch t := val.(type) {
	case float32:
		return float64(t), true
	case float64:
		return t, true
	default:
		return 0, false
	}
}

func (s *Series) GetBool(key string) (bool, bool) {
	val, exist := s.Get(key)
	if !exist {
		return false, exist
	}

	v, ok := val.(bool)
	return v, ok
}

func (s *Series) GetString(key string) (string, bool) {
	val, exist := s.Get(key)
	if !exist {
		return "", exist
	}

	v, ok := val.(string)
	return v, ok
}

func (s *Series) GetBytes(key string) ([]byte, bool) {
	val, exist := s.Get(key)
	if !exist {
		return nil, exist
	}

	v, ok := val.([]byte)
	return v, ok
}

func (s *Series) GetTimestamp(key string) (time.Time, bool) {
	val, exist := s.Get(key)
	if !exist {
		return time.Time{}, exist
	}

	v, ok := val.(time.Time)
	return v, ok
}

func (s *Series) add(name string, val any, semantic greptimepb.SemanticType) error {
	key, err := gutil.ToColumnName(name)
	if err != nil {
		return err
	}

	if s.columns == nil {
		s.columns = map[string]column{}
	}

	v, err := gutil.Convert(val)
	if err != nil {
		return fmt.Errorf("add tag err: %w", err)
	}

	newCol := column{
		typ:      v.Type,
		semantic: semantic,
	}
	if col, seen := s.columns[key]; seen {
		if err := checkColumnEquality(key, col, newCol); err != nil {
			return err
		}
	}
	s.columns[key] = newCol
	s.orders = append(s.orders, key)

	if s.vals == nil {
		s.vals = map[string]any{}
	}
	s.vals[key] = v.Val

	return nil
}

// AddTag prepare tag column, and old value will be replaced if same tag is set.
// the length of key CAN NOT be longer than 100.
// If you want to constrain the column type, you can directly use like:
//   - [Series.AddFloatTag]
//   - [Series.AddIntTag]
//   - ...
func (s *Series) AddTag(key string, val any) error {
	return s.add(key, val, greptimepb.SemanticType_TAG)
}

// AddFloatTag helps to constrain the key to be float64 type, if you want to
// add float32 tag instead of float64, you can do it like:
//
//	var i float32 = 1.0
//	return s.AddFloatTag("memory", float64(i))
func (s *Series) AddFloatTag(key string, val float64) error {
	return s.AddTag(key, val)
}

// AddIntTag helps to constrain the key to be int64 type, if you want to
// add int32 tag instead of int64, you can do it like:
//
//	var i int32 = 1
//	return s.AddIntTag("account", int64(i))
func (s *Series) AddIntTag(key string, val int64) error {
	return s.AddTag(key, val)
}

// AddUintTag helps to constrain the key to be uint64 type, if you want to
// add uint32 tag instead of uint64, you can do it like:
//
//	var i uint32 = 1
//	return s.AddUintTag("account", uint64(i))
func (s *Series) AddUintTag(key string, val uint64) error {
	return s.AddTag(key, val)
}

// AddBoolTag helps to constrain the key to be bool type
func (s *Series) AddBoolTag(key string, val bool) error {
	return s.AddTag(key, val)
}

// AddStringTag helps to constrain the key to be string type
func (s *Series) AddStringTag(key string, val string) error {
	return s.AddTag(key, val)
}

// AddBytesTag helps to constrain the key to be []byte type
func (s *Series) AddBytesTag(key string, val []byte) error {
	return s.AddTag(key, val)
}

// AddField prepare field column, and old value will be replaced if same field is set.
// the length of key CAN NOT be longer than 100
func (s *Series) AddField(key string, val any) error {
	return s.add(key, val, greptimepb.SemanticType_FIELD)
}

// AddFloatField helps to constrain the key to be float64 type, if you want to
// add float32 tag instead of float64, you can do it like:
//
//	var i float32 = 1.0
//	return s.AddFloatField("memory", float64(i))
func (s *Series) AddFloatField(key string, val float64) error {
	return s.AddField(key, val)
}

// AddIntField helps to constrain the key to be int64 type, if you want to
// add int32 tag instead of int64, you can do it like:
//
//	var i int32 = 1
//	return s.AddIntField("account", int64(i))
func (s *Series) AddIntField(key string, val int64) error {
	return s.AddField(key, val)
}

// AddUintField helps to constrain the key to be uint64 type, if you want to
// add uint32 tag instead of uint64, you can do it like:
//
//	var i uint32 = 1
//	return s.AddUintField("account", uint64(i))
func (s *Series) AddUintField(key string, val uint64) error {
	return s.AddField(key, val)
}

// AddBoolField helps to constrain the key to be bool type
func (s *Series) AddBoolField(key string, val bool) error {
	return s.AddField(key, val)
}

// AddStringField helps to constrain the key to be string type
func (s *Series) AddStringField(key string, val string) error {
	return s.AddField(key, val)
}

// AddBytesField helps to constrain the key to be []byte type
func (s *Series) AddBytesField(key string, val []byte) error {
	return s.AddField(key, val)
}

// SetTimestamp is required
func (s *Series) SetTimestamp(t time.Time) error {
	s.timestamp = t
	return nil
}
