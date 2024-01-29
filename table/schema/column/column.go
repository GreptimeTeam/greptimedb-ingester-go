package column

import greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

type Column struct {
	Name         string
	SemanticType greptimepb.SemanticType
	DataType     greptimepb.ColumnDataType
}

func New(name string, semanticType greptimepb.SemanticType, dataType greptimepb.ColumnDataType) Column {
	return Column{Name: name, SemanticType: semanticType, DataType: dataType}
}

func (col Column) ToColumnSchema() *greptimepb.ColumnSchema {
	return &greptimepb.ColumnSchema{
		ColumnName:   col.Name,
		SemanticType: col.SemanticType,
		Datatype:     col.DataType,
	}
}
