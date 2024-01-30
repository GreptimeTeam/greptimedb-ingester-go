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

func (s *Schema) IsEmpty() bool {
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
