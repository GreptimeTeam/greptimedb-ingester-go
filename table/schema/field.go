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
	"strings"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
	"github.com/GreptimeTeam/greptimedb-ingester-go/util"
)

type Field struct {
	Name         string             // default is field name
	SemanticType gpb.SemanticType   // default is field
	Datatype     gpb.ColumnDataType // default is the value type
}

func parseField(structField reflect.StructField) (*Field, error) {
	field := &Field{}
	tags := parseTag(structField.Tag.Get("greptime"), ";")

	columnName, err := util.SanitateName(structField.Name)
	if err != nil {
		return nil, err
	}
	if val, ok := tags["COLUMN"]; ok {
		columnName = val
	}
	field.Name = columnName

	if _, ok := tags["TAG"]; ok {
		field.SemanticType = gpb.SemanticType_TAG
	} else if _, ok := tags["TIMESTAMP"]; ok {
		field.SemanticType = gpb.SemanticType_TIMESTAMP
	} else {
		field.SemanticType = gpb.SemanticType_FIELD
	}

	typ, err := parseTypeKind(structField.Type)
	if err != nil {
		return nil, err
	}
	if val, ok := tags["TYPE"]; ok {
		typ_, err := types.ParseColumnType(val)
		if err != nil {
			return nil, err
		}
		typ = typ_
	}
	field.Datatype = typ

	return field, nil
}

func parseTag(str string, sep string) map[string]string {
	tags := map[string]string{}
	names := strings.Split(str, sep)

	for i := 0; i < len(names); i++ {
		values := strings.Split(names[i], ":")
		k := strings.TrimSpace(strings.ToUpper(values[0]))

		if len(values) >= 2 {
			tags[k] = strings.Join(values[1:], ":")
		} else if k != "" {
			tags[k] = k
		}
	}

	return tags
}

func parseTypeKind(typ reflect.Type) (gpb.ColumnDataType, error) {
	switch kind := typ.Kind(); kind {
	case reflect.Bool:
		return gpb.ColumnDataType_BOOLEAN, nil
	case reflect.Int:
		return gpb.ColumnDataType_INT64, nil
	case reflect.Int8:
		return gpb.ColumnDataType_INT8, nil
	case reflect.Int16:
		return gpb.ColumnDataType_INT16, nil
	case reflect.Int32:
		return gpb.ColumnDataType_INT32, nil
	case reflect.Int64:
		return gpb.ColumnDataType_INT64, nil
	case reflect.Uint:
		return gpb.ColumnDataType_UINT64, nil
	case reflect.Uint8:
		return gpb.ColumnDataType_UINT8, nil
	case reflect.Uint16:
		return gpb.ColumnDataType_UINT16, nil
	case reflect.Uint32:
		return gpb.ColumnDataType_UINT32, nil
	case reflect.Uint64:
		return gpb.ColumnDataType_UINT64, nil
	case reflect.Float32:
		return gpb.ColumnDataType_FLOAT32, nil
	case reflect.Float64:
		return gpb.ColumnDataType_FLOAT64, nil
	case reflect.Array, reflect.Slice: // only binary is supported
		elem := typ.Elem()
		if elem.Kind() == reflect.Uint8 {
			return gpb.ColumnDataType_BINARY, nil
		} else {
			return -1, fmt.Errorf("unsupported type %q", kind.String())
		}
	case reflect.Pointer:
		return parseTypeKind(typ.Elem())
	case reflect.String:
		return gpb.ColumnDataType_STRING, nil
	case reflect.Struct:
		if typ.PkgPath() == "time" && typ.Name() == "Time" {
			return gpb.ColumnDataType_TIMESTAMP_MILLISECOND, nil
		} else {
			return -1, fmt.Errorf("unsupported type %q", kind.String())
		}

	default:
		return -1, fmt.Errorf("unsupported type %q", kind.String())
	}
}
