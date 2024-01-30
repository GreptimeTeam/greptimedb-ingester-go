package cell

import (
	"fmt"
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

const (
	ONE_DAY_IN_SECONDS int32 = 86400

	formatter = "the type '%T' of value %#v is not"
)

func BuildBool(v any) (*gpb.Value, error) {
	switch t := v.(type) {
	case bool:
		return &gpb.Value{ValueData: &gpb.Value_BoolValue{BoolValue: t}}, nil
	case *bool:
		return &gpb.Value{ValueData: &gpb.Value_BoolValue{BoolValue: *t}}, nil
	default:
		return nil, fmt.Errorf(formatter+" bool", t, v)
	}
}

func BuildString(v any) (*gpb.Value, error) {
	switch t := v.(type) {
	case string:
		return &gpb.Value{ValueData: &gpb.Value_StringValue{StringValue: t}}, nil
	case *string:
		return &gpb.Value{ValueData: &gpb.Value_StringValue{StringValue: *t}}, nil
	default:
		return nil, fmt.Errorf(formatter+" string", t, v)
	}
}

func BuildBytes(v any) (*gpb.Value, error) {
	switch t := v.(type) {
	case []byte:
		return &gpb.Value{ValueData: &gpb.Value_BinaryValue{BinaryValue: t}}, nil
	case *[]byte:
		return &gpb.Value{ValueData: &gpb.Value_BinaryValue{BinaryValue: *t}}, nil
	default:
		return nil, fmt.Errorf(formatter+" bytes", t, v)
	}
}

func getIntPointer(v any) (*int32, *int64, *uint32, *uint64, error) {
	var int32Val *int32
	var int64Val *int64
	var uint32Val *uint32
	var uint64Val *uint64
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
		return nil, nil, nil, nil, fmt.Errorf(formatter+" Integer", t, v)
	}

	return int32Val, int64Val, uint32Val, uint64Val, nil
}

func getInt32Value(int32Pointer *int32, int64Pointer *int64, uint32Pointer *uint32, uint64Pointer *uint64) int32 {
	if int32Pointer != nil {
		return *int32Pointer
	}
	if int64Pointer != nil {
		return int32(*int64Pointer)
	}
	if uint32Pointer != nil {
		return int32(*uint32Pointer)
	}
	if uint64Pointer != nil {
		return int32(*uint64Pointer)
	}
	return 0
}

func getInt64Value(int32Pointer *int32, int64Pointer *int64, uint32Pointer *uint32, uint64Pointer *uint64) int64 {
	if int32Pointer != nil {
		return int64(*int32Pointer)
	}
	if int64Pointer != nil {
		return *int64Pointer
	}
	if uint32Pointer != nil {
		return int64(*uint32Pointer)
	}
	if uint64Pointer != nil {
		return int64(*uint64Pointer)
	}
	return 0
}

func getUint32Value(int32Pointer *int32, int64Pointer *int64, uint32Pointer *uint32, uint64Pointer *uint64) uint32 {
	if int32Pointer != nil {
		return uint32(*int32Pointer)
	}
	if int64Pointer != nil {
		return uint32(*int64Pointer)
	}
	if uint32Pointer != nil {
		return *uint32Pointer
	}
	if uint64Pointer != nil {
		return uint32(*uint64Pointer)
	}
	return 0
}

func getUint64Value(int32Pointer *int32, int64Pointer *int64, uint32Pointer *uint32, uint64Pointer *uint64) uint64 {
	if int32Pointer != nil {
		return uint64(*int32Pointer)
	}
	if int64Pointer != nil {
		return uint64(*int64Pointer)
	}
	if uint32Pointer != nil {
		return uint64(*uint32Pointer)
	}
	if uint64Pointer != nil {
		return *uint64Pointer
	}
	return 0
}

func BuildInt(v any, t gpb.ColumnDataType) (*gpb.Value, error) {
	int32Pointer, int64Pointer, uint32Pointer, uint64Pointer, err := getIntPointer(v)
	if err != nil {
		return nil, err
	}

	switch t {
	case gpb.ColumnDataType_INT8:
		val := getInt32Value(int32Pointer, int64Pointer, uint32Pointer, uint64Pointer)
		return &gpb.Value{ValueData: &gpb.Value_I8Value{I8Value: val}}, nil
	case gpb.ColumnDataType_INT16:
		val := getInt32Value(int32Pointer, int64Pointer, uint32Pointer, uint64Pointer)
		return &gpb.Value{ValueData: &gpb.Value_I16Value{I16Value: val}}, nil
	case gpb.ColumnDataType_INT32:
		val := getInt32Value(int32Pointer, int64Pointer, uint32Pointer, uint64Pointer)
		return &gpb.Value{ValueData: &gpb.Value_I32Value{I32Value: val}}, nil
	case gpb.ColumnDataType_INT64:
		val := getInt64Value(int32Pointer, int64Pointer, uint32Pointer, uint64Pointer)
		return &gpb.Value{ValueData: &gpb.Value_I64Value{I64Value: val}}, nil
	default:
		return nil, fmt.Errorf(formatter+" Integer", t, v)
	}
}

func BuildUint(v any, t gpb.ColumnDataType) (*gpb.Value, error) {
	int32Pointer, int64Pointer, uint32Pointer, uint64Pointer, err := getIntPointer(v)
	if err != nil {
		return nil, err
	}

	switch t {
	case gpb.ColumnDataType_UINT8:
		val := getUint32Value(int32Pointer, int64Pointer, uint32Pointer, uint64Pointer)
		return &gpb.Value{ValueData: &gpb.Value_U8Value{U8Value: val}}, nil
	case gpb.ColumnDataType_UINT16:
		val := getUint32Value(int32Pointer, int64Pointer, uint32Pointer, uint64Pointer)
		return &gpb.Value{ValueData: &gpb.Value_U16Value{U16Value: val}}, nil
	case gpb.ColumnDataType_UINT32:
		val := getUint32Value(int32Pointer, int64Pointer, uint32Pointer, uint64Pointer)
		return &gpb.Value{ValueData: &gpb.Value_U32Value{U32Value: val}}, nil
	case gpb.ColumnDataType_UINT64:
		val := getUint64Value(int32Pointer, int64Pointer, uint32Pointer, uint64Pointer)
		return &gpb.Value{ValueData: &gpb.Value_U64Value{U64Value: val}}, nil
	default:
		return nil, fmt.Errorf(formatter+" Unsigned Integer", t, v)
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
		return nil, nil, fmt.Errorf(formatter+" Float", t, v)
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

func BuildFloat(v any, t gpb.ColumnDataType) (*gpb.Value, error) {
	f32Pointer, f64Pointer, err := getFloatPointer(v)
	if err != nil {
		return nil, err
	}

	switch t {
	case gpb.ColumnDataType_FLOAT32:
		return &gpb.Value{ValueData: &gpb.Value_F32Value{F32Value: getFloat32Value(f32Pointer, f64Pointer)}}, nil
	case gpb.ColumnDataType_FLOAT64:
		return &gpb.Value{ValueData: &gpb.Value_F64Value{F64Value: getFloat64Value(f32Pointer, f64Pointer)}}, nil
	default:
		return nil, fmt.Errorf(formatter+" Float", t, v)
	}
}

func getTime(v any) (*time.Time, error) {
	switch t := v.(type) {
	case time.Time:
		return &t, nil
	case *time.Time:
		return t, nil
	default:
		return nil, fmt.Errorf(formatter+" time.Time", t, v)
	}
}

func getTimeOrInteger(v any) (*time.Time, *int64, error) {
	t, _ := getTime(v) // ignore getTime error, try getIntPointer instead
	if t != nil {
		return t, nil, nil
	}

	int32Pointer, int64Pointer, uint32Pointer, uint64Pointer, err := getIntPointer(v)
	if err != nil {
		return nil, nil, fmt.Errorf(formatter+" Time or Integer", v, v)
	}
	i := getInt64Value(int32Pointer, int64Pointer, uint32Pointer, uint64Pointer)
	return nil, &i, nil
}

func BuildDate(v any) (*gpb.Value, error) {
	t, i, err := getTimeOrInteger(v)
	if err != nil {
		return nil, err
	}

	var val int32
	if t != nil {
		val = int32(t.Unix()) / ONE_DAY_IN_SECONDS
	} else {
		val = int32(*i)
	}

	return &gpb.Value{ValueData: &gpb.Value_DateValue{DateValue: val}}, nil
}

func BuildDateTime(v any) (*gpb.Value, error) {
	t, i, err := getTimeOrInteger(v)
	if err != nil {
		return nil, err
	}

	var val int64
	if t != nil {
		val = t.UnixMilli()
	} else {
		val = *i
	}

	return &gpb.Value{ValueData: &gpb.Value_DatetimeValue{DatetimeValue: val}}, nil
}

func BuildTimestampSecond(v any) (*gpb.Value, error) {
	t, i, err := getTimeOrInteger(v)
	if err != nil {
		return nil, err
	}

	var val int64
	if t != nil {
		val = t.Unix()
	} else {
		val = *i
	}

	return &gpb.Value{ValueData: &gpb.Value_TimestampSecondValue{TimestampSecondValue: val}}, nil
}

func BuildTimestampMillisecond(v any) (*gpb.Value, error) {
	t, i, err := getTimeOrInteger(v)
	if err != nil {
		return nil, err
	}

	var val int64
	if t != nil {
		val = t.UnixMilli()
	} else {
		val = *i
	}

	return &gpb.Value{ValueData: &gpb.Value_TimestampMillisecondValue{TimestampMillisecondValue: val}}, nil
}

func BuildTimestampMicrosecond(v any) (*gpb.Value, error) {
	t, i, err := getTimeOrInteger(v)
	if err != nil {
		return nil, err
	}

	var val int64
	if t != nil {
		val = t.UnixMicro()
	} else {
		val = *i
	}

	return &gpb.Value{ValueData: &gpb.Value_TimestampMicrosecondValue{TimestampMicrosecondValue: val}}, nil
}

func BuildTimestampNanosecond(v any) (*gpb.Value, error) {
	t, i, err := getTimeOrInteger(v)
	if err != nil {
		return nil, err
	}

	var val int64
	if t != nil {
		val = t.UnixNano()
	} else {
		val = *i
	}

	return &gpb.Value{ValueData: &gpb.Value_TimestampNanosecondValue{TimestampNanosecondValue: val}}, nil
}

func BuildTimeSecond(v any) (*gpb.Value, error) {
	t, i, err := getTimeOrInteger(v)
	if err != nil {
		return nil, err
	}

	var val int64
	if t != nil {
		val = t.Unix()
	} else {
		val = *i
	}

	return &gpb.Value{ValueData: &gpb.Value_TimeSecondValue{TimeSecondValue: val}}, nil
}

func BuildTimeMillisecond(v any) (*gpb.Value, error) {
	t, i, err := getTimeOrInteger(v)
	if err != nil {
		return nil, err
	}

	var val int64
	if t != nil {
		val = t.UnixMilli()
	} else {
		val = *i
	}

	return &gpb.Value{ValueData: &gpb.Value_TimeMillisecondValue{TimeMillisecondValue: val}}, nil
}

func BuildTimeMicrosecond(v any) (*gpb.Value, error) {
	t, i, err := getTimeOrInteger(v)
	if err != nil {
		return nil, err
	}

	var val int64
	if t != nil {
		val = t.UnixMicro()
	} else {
		val = *i
	}

	return &gpb.Value{ValueData: &gpb.Value_TimeMicrosecondValue{TimeMicrosecondValue: val}}, nil
}

func BuildTimeNanosecond(v any) (*gpb.Value, error) {
	t, i, err := getTimeOrInteger(v)
	if err != nil {
		return nil, err
	}

	var val int64
	if t != nil {
		val = t.UnixNano()
	} else {
		val = *i
	}

	return &gpb.Value{ValueData: &gpb.Value_TimeNanosecondValue{TimeNanosecondValue: val}}, nil
}
