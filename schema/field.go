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
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	"github.com/GreptimeTeam/greptimedb-ingester-go/table/cell"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
	"github.com/GreptimeTeam/greptimedb-ingester-go/util"
)

type Field struct {
	Name         string             // default is field name
	SemanticType gpb.SemanticType   // default is field
	Datatype     gpb.ColumnDataType // default is the value type
}

func (f Field) ToColumnSchema() *gpb.ColumnSchema {
	return &gpb.ColumnSchema{
		ColumnName:   f.Name,
		SemanticType: f.SemanticType,
		Datatype:     f.Datatype,
	}
}

func newField(columnName string, semanticType gpb.SemanticType, datatype gpb.ColumnDataType) *Field {
	return &Field{Name: columnName, SemanticType: semanticType, Datatype: datatype}
}

func newColumnSchema(columnName string, semanticType gpb.SemanticType, datatype gpb.ColumnDataType) *gpb.ColumnSchema {
	return newField(columnName, semanticType, datatype).ToColumnSchema()
}

func parseField(structField reflect.StructField) (*Field, error) {
	tags := parseTag(structField)

	if _, ok := tags["-"]; ok && len(tags) == 1 {
		return nil, nil
	}

	columnName, err := util.SanitateName(structField.Name)
	if err != nil {
		return nil, err
	}
	if col, ok := tags["COLUMN"]; ok {
		columnName = col
	}

	semanticType := gpb.SemanticType_FIELD
	if _, ok := tags["TAG"]; ok {
		semanticType = gpb.SemanticType_TAG
	} else if _, ok := tags["TIMESTAMP"]; ok {
		semanticType = gpb.SemanticType_TIMESTAMP
	}

	typ, err := parseType(structField.Type)
	if err != nil {
		return nil, err
	}
	if val, ok := tags["TYPE"]; ok {
		typ_, err := types.ParseColumnType(val, tags["PRECISION"])
		if err != nil {
			return nil, err
		}
		typ = typ_
	}

	return newField(columnName, semanticType, typ), nil
}

func parseTag(structField reflect.StructField) map[string]string {
	tags := map[string]string{}

	str, ok := structField.Tag.Lookup("greptime")
	if !ok {
		return tags
	}

	names := strings.Split(str, ";")
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

func parseType(typ reflect.Type) (gpb.ColumnDataType, error) {
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
		return parseType(typ.Elem())
	case reflect.String:
		return gpb.ColumnDataType_STRING, nil
	case reflect.Struct:
		if isTimeType(typ) {
			return gpb.ColumnDataType_TIMESTAMP_MILLISECOND, nil
		} else {
			return -1, fmt.Errorf("unsupported type %q", kind.String())
		}
	default:
		return -1, fmt.Errorf("unsupported type %q", kind.String())
	}
}

// parseIntOrTimeValue assumes val is a time.Time type or a integer type or a unsigned integer type
func parseIntOrTimeValue(typ gpb.ColumnDataType, val reflect.Value) (*gpb.Value, error) {
	if val.CanInt() {
		return cell.New(val.Int(), typ).Build()
	}

	if val.CanUint() {
		return cell.New(val.Uint(), typ).Build()
	}

	if isTimeType(val.Type()) {
		method := val.MethodByName("UnixNano")
		if !method.IsValid() {
			return nil, fmt.Errorf("type %T of %#v does not have UnixNano method", val, val)
		}
		results := method.Call([]reflect.Value{})
		t := time.Unix(0, results[0].Int())
		return cell.New(t, typ).Build()
	}

	return nil, fmt.Errorf("unsupported type %T of %#v", val, val)
}

func parseValue(typ gpb.ColumnDataType, val reflect.Value) (*gpb.Value, error) {
	val = reflect.Indirect(val)
	if !val.IsValid() {
		return nil, nil
	}

	switch typ {
	case gpb.ColumnDataType_INT8, gpb.ColumnDataType_INT16, gpb.ColumnDataType_INT32, gpb.ColumnDataType_INT64:
		if !val.CanInt() {
			return nil, fmt.Errorf("%#v is not compatible with Int", val)
		}
		return cell.New(val.Int(), typ).Build()

	case gpb.ColumnDataType_UINT8, gpb.ColumnDataType_UINT16, gpb.ColumnDataType_UINT32, gpb.ColumnDataType_UINT64:
		if !val.CanUint() {
			return nil, fmt.Errorf("%#v is not compatible with Unsigned Int", val)
		}
		return cell.New(val.Uint(), typ).Build()

	case gpb.ColumnDataType_FLOAT32, gpb.ColumnDataType_FLOAT64:
		if !val.CanFloat() {
			return nil, fmt.Errorf("%#v is not compatible with Float", val)
		}
		return cell.New(val.Float(), typ).Build()

	case gpb.ColumnDataType_BOOLEAN:
		if val.Kind() != reflect.Bool {
			return nil, fmt.Errorf("%#v is not compatible with Bool", val)
		}
		return cell.New(val.Bool(), typ).Build()

	case gpb.ColumnDataType_BINARY:
		if (val.Kind() != reflect.Slice && val.Kind() != reflect.Array) ||
			val.Type().Elem().Kind() != reflect.Uint8 {
			return nil, fmt.Errorf("%#v is not compatible with Bytes", val)
		}
		return cell.New(val.Bytes(), typ).Build()

	case gpb.ColumnDataType_STRING:
		if val.Kind() != reflect.String {
			return nil, fmt.Errorf("%#v is not compatible with String", val)
		}
		return cell.New(val.String(), typ).Build()

	case gpb.ColumnDataType_DATE:
		return parseIntOrTimeValue(typ, val)
	case gpb.ColumnDataType_DATETIME:
		return parseIntOrTimeValue(typ, val)

	case gpb.ColumnDataType_TIMESTAMP_SECOND:
		return parseIntOrTimeValue(typ, val)
	case gpb.ColumnDataType_TIMESTAMP_MILLISECOND:
		return parseIntOrTimeValue(typ, val)
	case gpb.ColumnDataType_TIMESTAMP_MICROSECOND:
		return parseIntOrTimeValue(typ, val)
	case gpb.ColumnDataType_TIMESTAMP_NANOSECOND:
		return parseIntOrTimeValue(typ, val)

	case gpb.ColumnDataType_TIME_SECOND:
		return parseIntOrTimeValue(typ, val)
	case gpb.ColumnDataType_TIME_MILLISECOND:
		return parseIntOrTimeValue(typ, val)
	case gpb.ColumnDataType_TIME_MICROSECOND:
		return parseIntOrTimeValue(typ, val)
	case gpb.ColumnDataType_TIME_NANOSECOND:
		return parseIntOrTimeValue(typ, val)

	case gpb.ColumnDataType_INTERVAL_YEAR_MONTH,
		gpb.ColumnDataType_INTERVAL_DAY_TIME,
		gpb.ColumnDataType_INTERVAL_MONTH_DAY_NANO:
		return nil, fmt.Errorf("INTERVAL not implemented yet for %#v", val)

	// TODO(yuanbohan): support decimal 128
	case gpb.ColumnDataType_DECIMAL128:
		return nil, fmt.Errorf("DECIMAL 128 not supported for %#v", val)

	case gpb.ColumnDataType_JSON:
		if val.Kind() != reflect.String {
			return nil, fmt.Errorf("%#v is not compatible with String", val)
		}
		return cell.New(val.String(), typ).Build()

	default:
		return nil, fmt.Errorf("unknown column data type: %v", typ)
	}
}

func isTimeType(typ reflect.Type) bool {
	return typ.PkgPath() == "time" && typ.Name() == "Time"
}
