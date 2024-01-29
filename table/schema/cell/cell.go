package cell

import (
	"fmt"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type Cell struct {
	Val      any
	DataType gpb.ColumnDataType
}

func New(v any, dataType gpb.ColumnDataType) Cell {
	return Cell{Val: v, DataType: dataType}
}

func (c Cell) Build() (*gpb.Value, error) {
	if c.Val == nil {
		return &gpb.Value{}, nil
	}

	switch c.DataType {
	case gpb.ColumnDataType_BOOLEAN:
		return BuildBool(c.Val)

	case gpb.ColumnDataType_INT8, gpb.ColumnDataType_INT16, gpb.ColumnDataType_INT32, gpb.ColumnDataType_INT64:
		return BuildInt(c.Val, c.DataType)

	case gpb.ColumnDataType_UINT8, gpb.ColumnDataType_UINT16, gpb.ColumnDataType_UINT32, gpb.ColumnDataType_UINT64:
		return BuildUint(c.Val, c.DataType)

	case gpb.ColumnDataType_FLOAT32, gpb.ColumnDataType_FLOAT64:
		return BuildFloat(c.Val, c.DataType)

	case gpb.ColumnDataType_BINARY:
		return BuildBytes(c.Val)

	case gpb.ColumnDataType_STRING:
		return BuildString(c.Val)

	case gpb.ColumnDataType_DATE:
	case gpb.ColumnDataType_DATETIME:

	case gpb.ColumnDataType_TIMESTAMP_SECOND:
	case gpb.ColumnDataType_TIMESTAMP_MILLISECOND:
	case gpb.ColumnDataType_TIMESTAMP_MICROSECOND:
	case gpb.ColumnDataType_TIMESTAMP_NANOSECOND:

	case gpb.ColumnDataType_TIME_SECOND:
	case gpb.ColumnDataType_TIME_MILLISECOND:
	case gpb.ColumnDataType_TIME_MICROSECOND:
	case gpb.ColumnDataType_TIME_NANOSECOND:

	case gpb.ColumnDataType_INTERVAL_YEAR_MONTH:
	case gpb.ColumnDataType_INTERVAL_DAY_TIME:
	case gpb.ColumnDataType_INTERVAL_MONTH_DAY_NANO:
	default:
		return nil, fmt.Errorf("unknown column data type: %v", c.DataType)
	}

	return nil, nil
}
