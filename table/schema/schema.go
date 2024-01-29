package schema

import (
	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	"github.com/GreptimeTeam/greptimedb-ingester-go/table/schema/column"
)

type Schema struct {
	Name    string
	Columns []column.Column
}

func New(name string) *Schema {
	return &Schema{Name: name}
}

func (s *Schema) IsZero() bool {
	return s.Columns == nil || len(s.Columns) == 0
}

func (s *Schema) GetColumnCount() int {
	return len(s.Columns)
}

func (s *Schema) GetColumn(idx int) column.Column {
	return s.Columns[idx]
}

func (s *Schema) AddColumn(name string, semanticType gpb.SemanticType, dataType gpb.ColumnDataType) {
	column := column.New(name, semanticType, dataType)
	s.Columns = append(s.Columns, column)
}
