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
	"fmt"
	"reflect"

	"github.com/GreptimeTeam/greptimedb-ingester-go/util"
)

type Schema struct {
	Name   string
	Fields []Field
}

type Tabler interface {
	TableName() string
}

func parseSchema(dest any) (*Schema, error) {
	if dest == nil {
		return nil, fmt.Errorf("unsupported data type: %+v", dest)
	}

	value := reflect.ValueOf(dest)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		value = reflect.New(value.Type().Elem())
	}

	typ := reflect.Indirect(value).Type()

	if typ.Kind() == reflect.Interface {
		typ = reflect.Indirect(reflect.ValueOf(dest)).Elem().Type()
	}

	for typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array || typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		if typ.PkgPath() == "" {
			return nil, fmt.Errorf("unsupported data type: %+v", dest)
		}
		return nil, fmt.Errorf("unsupported data type: %s.%s", typ.PkgPath(), typ.Name())
	}

	val := reflect.New(typ)
	tableName, err := util.SanitateName(typ.Name())
	if err != nil {
		return nil, err
	}

	if tabler, ok := val.Interface().(Tabler); ok {
		tableName = tabler.TableName()
	}

	schema := &Schema{
		Name:   tableName,
		Fields: []Field{},
	}

	for i := 0; i < typ.NumField(); i++ {
		if structField := typ.Field(i); structField.IsExported() {
			field, err := parseField(structField)
			if err != nil {
				return nil, err
			}
			schema.Fields = append(schema.Fields, *field)
		}
	}

	return schema, nil
}
