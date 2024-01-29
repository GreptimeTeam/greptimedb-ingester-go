package cell

import (
	"fmt"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

func BuildBool(v any) (*gpb.Value, error) {
	switch t := v.(type) {
	case bool:
		return &gpb.Value{ValueData: &gpb.Value_BoolValue{BoolValue: t}}, nil
	case *bool:
		return &gpb.Value{ValueData: &gpb.Value_BoolValue{BoolValue: *t}}, nil
	default:
		return nil, fmt.Errorf("%v is not bool", v)
	}
}

func BuildString(v any) (*gpb.Value, error) {
	switch t := v.(type) {
	case string:
		return &gpb.Value{ValueData: &gpb.Value_StringValue{StringValue: t}}, nil
	case *string:
		return &gpb.Value{ValueData: &gpb.Value_StringValue{StringValue: *t}}, nil
	default:
		return nil, fmt.Errorf("%v is not string", v)
	}
}

func BuildBytes(v any) (*gpb.Value, error) {
	switch t := v.(type) {
	case []byte:
		return &gpb.Value{ValueData: &gpb.Value_BinaryValue{BinaryValue: t}}, nil
	case *[]byte:
		return &gpb.Value{ValueData: &gpb.Value_BinaryValue{BinaryValue: *t}}, nil
	default:
		return nil, fmt.Errorf("%v is not bytes", v)
	}
}

func getIntPointer(v any) (*int32, *int64, error) {
	var int32Val *int32
	var int64Val *int64
	switch t := v.(type) {
	case int64:
		int64Val = &t
	case int32:
		int32Val = &t
	case int16:
		val := int32(t)
		int32Val = &val
	case int8:
		val := int32(t)
		int32Val = &val
	case int:
		val := int64(t)
		int64Val = &val

	case *int64:
		int64Val = t
	case *int32:
		int32Val = t
	case *int16:
		val := int32(*t)
		int32Val = &val
	case *int8:
		val := int32(*t)
		int32Val = &val
	case *int:
		val := int64(*t)
		int64Val = &val

	default:
		return nil, nil, fmt.Errorf("the type '%T' of value %v is not Int", t, v)
	}

	return int32Val, int64Val, nil
}

func getInt32Value(int32Val *int32, int64Val *int64) int32 {
	if int32Val != nil {
		return *int32Val
	}
	if int64Val != nil {
		return int32(*int64Val)
	}
	return 0
}

func getInt64Value(int32Val *int32, int64Val *int64) int64 {
	if int32Val != nil {
		return int64(*int32Val)
	}
	if int64Val != nil {
		return *int64Val
	}
	return 0
}

func BuildInt(v any, typ gpb.ColumnDataType) (*gpb.Value, error) {
	int32Pointer, int64Pointer, err := getIntPointer(v)
	if err != nil {
		return nil, err
	}

	switch typ {
	case gpb.ColumnDataType_INT8:
		return &gpb.Value{ValueData: &gpb.Value_I8Value{I8Value: getInt32Value(int32Pointer, int64Pointer)}}, nil
	case gpb.ColumnDataType_INT16:
		return &gpb.Value{ValueData: &gpb.Value_I16Value{I16Value: getInt32Value(int32Pointer, int64Pointer)}}, nil
	case gpb.ColumnDataType_INT32:
		return &gpb.Value{ValueData: &gpb.Value_I32Value{I32Value: getInt32Value(int32Pointer, int64Pointer)}}, nil
	case gpb.ColumnDataType_INT64:
		return &gpb.Value{ValueData: &gpb.Value_I64Value{I64Value: getInt64Value(int32Pointer, int64Pointer)}}, nil
	default:
		return nil, fmt.Errorf("the type '%T' is not Int", typ)
	}
}

func getUintPointer(v any) (*uint32, *uint64, error) {
	var uint32Val *uint32
	var uint64Val *uint64
	switch t := v.(type) {
	case uint64:
		uint64Val = &t
	case uint32:
		uint32Val = &t
	case uint16:
		val := uint32(t)
		uint32Val = &val
	case uint8:
		val := uint32(t)
		uint32Val = &val
	case uint:
		val := uint64(t)
		uint64Val = &val

	case *uint64:
		uint64Val = t
	case *uint32:
		uint32Val = t
	case *uint16:
		val := uint32(*t)
		uint32Val = &val
	case *uint8:
		val := uint32(*t)
		uint32Val = &val
	case *uint:
		val := uint64(*t)
		uint64Val = &val

	default:
		return nil, nil, fmt.Errorf("the type '%T' of value %v is not Unsigned Int", t, v)
	}

	return uint32Val, uint64Val, nil
}

func getUint32Value(uint32Val *uint32, uint64Val *uint64) uint32 {
	if uint32Val != nil {
		return *uint32Val
	}
	if uint64Val != nil {
		return uint32(*uint64Val)
	}
	return 0
}

func getUint64Value(uint32Val *uint32, uint64Val *uint64) uint64 {
	if uint32Val != nil {
		return uint64(*uint32Val)
	}
	if uint64Val != nil {
		return *uint64Val
	}
	return 0
}

func BuildUint(v any, typ gpb.ColumnDataType) (*gpb.Value, error) {
	uint32Pointer, uint64Pointer, err := getUintPointer(v)
	if err != nil {
		return nil, err
	}

	switch typ {
	case gpb.ColumnDataType_UINT8:
		return &gpb.Value{ValueData: &gpb.Value_U8Value{U8Value: getUint32Value(uint32Pointer, uint64Pointer)}}, nil
	case gpb.ColumnDataType_UINT16:
		return &gpb.Value{ValueData: &gpb.Value_U16Value{U16Value: getUint32Value(uint32Pointer, uint64Pointer)}}, nil
	case gpb.ColumnDataType_UINT32:
		return &gpb.Value{ValueData: &gpb.Value_U32Value{U32Value: getUint32Value(uint32Pointer, uint64Pointer)}}, nil
	case gpb.ColumnDataType_UINT64:
		return &gpb.Value{ValueData: &gpb.Value_U64Value{U64Value: getUint64Value(uint32Pointer, uint64Pointer)}}, nil
	default:
		return nil, fmt.Errorf("the type '%T' is not Unsigned Int", typ)
	}
}

func getFloatPointer(v any) (*float32, *float64, error) {
	var f32Val *float32
	var f64Val *float64
	switch t := v.(type) {
	case float64:
		f64Val = &t
	case float32:
		f32Val = &t

	case *float64:
		f64Val = t
	case *float32:
		f32Val = t

	default:
		return nil, nil, fmt.Errorf("the type '%T' of value %v is not float", t, v)
	}

	return f32Val, f64Val, nil
}

func getFloat32Value(f32Val *float32, f64Val *float64) float32 {
	if f32Val != nil {
		return *f32Val
	}
	if f64Val != nil {
		return float32(*f64Val)
	}
	return 0
}

func getFloat64Value(f32Val *float32, f64Val *float64) float64 {
	if f32Val != nil {
		return float64(*f32Val)
	}
	if f64Val != nil {
		return *f64Val
	}
	return 0
}

func BuildFloat(v any, typ gpb.ColumnDataType) (*gpb.Value, error) {
	f32Pointer, f64Pointer, err := getFloatPointer(v)
	if err != nil {
		return nil, err
	}

	switch typ {
	case gpb.ColumnDataType_FLOAT32:
		return &gpb.Value{ValueData: &gpb.Value_F32Value{F32Value: getFloat32Value(f32Pointer, f64Pointer)}}, nil
	case gpb.ColumnDataType_FLOAT64:
		return &gpb.Value{ValueData: &gpb.Value_F64Value{F64Value: getFloat64Value(f32Pointer, f64Pointer)}}, nil
	default:
		return nil, fmt.Errorf("the type '%T' is not Unsigned Int", typ)
	}
}
