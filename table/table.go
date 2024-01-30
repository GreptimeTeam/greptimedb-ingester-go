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

package table

import (
	"fmt"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	"github.com/GreptimeTeam/greptimedb-ingester-go/errs"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/schema"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/schema/cell"
)

type Table struct {
	Schema schema.Schema
	Rows   *gpb.Rows
}

func New(schema schema.Schema) *Table {
	colSchema := make([]*gpb.ColumnSchema, len(schema.Columns))
	for _, col := range schema.Columns {
		colSchema = append(colSchema, col.ToColumnSchema())
	}

	return &Table{
		Schema: schema,
		Rows: &gpb.Rows{
			Schema: colSchema,
			Rows:   make([]*gpb.Row, 0),
		},
	}
}

func (t *Table) addRow(row *gpb.Row) {
	if t.Rows.Rows == nil {
		t.Rows.Rows = make([]*gpb.Row, 0)
	}

	t.Rows.Rows = append(t.Rows.Rows, row)
}

// AddRow will check if the input matches the schema
func (t *Table) AddRow(inputs ...any) error {
	if t.Schema.IsEmpty() {
		return errs.ErrColumnNotSet
	}

	if len(inputs) != t.Schema.GetColumnCount() {
		return fmt.Errorf("number of inputs %d does not match number of columns in schema %d", len(inputs), t.Schema.GetColumnCount())
	}

	row := gpb.Row{
		Values: make([]*gpb.Value, len(inputs)),
	}

	for i, input := range inputs {
		dataType := t.Schema.GetColumn(i).DataType
		val, err := cell.New(input, dataType).Build()
		if err != nil {
			return err
		}
		row.Values = append(row.Values, val)
	}

	t.addRow(&row)

	return nil
}

func (t *Table) IsEmpty() bool {
	return t.Schema.IsEmpty() || t.Rows.Rows == nil || len(t.Rows.Rows) == 0
}
