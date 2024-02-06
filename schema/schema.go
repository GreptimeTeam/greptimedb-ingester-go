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
	"errors"
	"fmt"
	"reflect"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/util"
)

type Schema struct {
	tableName string

	fields []*gpb.ColumnSchema
	values []*gpb.Row
}

type Tabler interface {
	TableName() string
}

func getTableName(typ reflect.Type) (string, error) {
	val := reflect.New(typ)
	tableName, err := util.SanitateName(typ.Name())
	if err != nil {
		return "", err
	}

	if tabler, ok := val.Interface().(Tabler); ok {
		tableName = tabler.TableName()
	}

	return tableName, nil
}

func Parse(input any) (*table.Table, error) {
	schema_, err := parseSchema(input)
	if err != nil {
		return nil, err
	}

	if err := schema_.parseValues(input); err != nil {
		return nil, err
	}

	return schema_.ToTable()
}

func indirectStruct(input any) (reflect.Type, error) {
	value := reflect.ValueOf(input)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		value = reflect.New(value.Type().Elem())
	}

	typ := reflect.Indirect(value).Type()

	if typ.Kind() == reflect.Interface {
		typ = reflect.Indirect(reflect.ValueOf(input)).Elem().Type()
	}

	for typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array || typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		if typ.PkgPath() == "" {
			return nil, fmt.Errorf("unsupported data type: %+v", input)
		}
		return nil, fmt.Errorf("unsupported data type: %s.%s", typ.PkgPath(), typ.Name())
	}
	return typ, nil
}

func parseSchema(input any) (*Schema, error) {
	if input == nil {
		return nil, fmt.Errorf("unsupported data type: %+v", input)
	}

	typ, err := indirectStruct(input)
	if err != nil {
		return nil, err
	}

	tableName, err := getTableName(typ)
	if err != nil {
		return nil, err
	}

	size := len(reflect.VisibleFields(typ))
	fields := make([]*gpb.ColumnSchema, 0, size)
	for _, structField := range reflect.VisibleFields(typ) {
		if !structField.IsExported() {
			continue
		}

		field, err := parseField(structField)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field.ToColumnSchema())
	}

	return &Schema{tableName: tableName, fields: fields}, nil
}

func (s *Schema) parseValues(input any) error {
	val := reflect.ValueOf(input)
	if val.Kind() == reflect.Ptr && val.IsNil() {
		return errors.New("unable to parse value from nil pointer")
	}

	val = reflect.Indirect(val)
	typ := val.Type()

	if typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		for i := 0; i < val.Len(); i++ {
			if err := s.parseValues(val.Index(i).Interface()); err != nil {
				return err
			}
		}
		return nil
	}

	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("unsupported type %T of %+v", input, input)
	}

	size := len(reflect.VisibleFields(typ))
	values := make([]*gpb.Value, 0, size)
	for i, structField := range reflect.VisibleFields(typ) {
		if !structField.IsExported() {
			continue
		}

		field := s.fields[i]

		value, err := parseValue(field.Datatype, val.FieldByName(structField.Name))
		if err != nil {
			return err
		}
		values = append(values, value)
	}

	if s.values == nil {
		s.values = make([]*gpb.Row, 0)
	}
	s.values = append(s.values, &gpb.Row{Values: values})
	return nil
}

func (s *Schema) ToTable() (*table.Table, error) {
	table_, err := table.New(s.tableName)
	if err != nil {
		return nil, err
	}
	return table_.WithColumnsSchema(s.fields).WithRows(&gpb.Rows{Rows: s.values}), nil
}
