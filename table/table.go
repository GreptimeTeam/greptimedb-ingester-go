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
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/cell"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
	"github.com/GreptimeTeam/greptimedb-ingester-go/util"
)

// Table is a struct that holds the table name, columns, and rows.
// Call New() to create a new table.
// then Call AddTagColumn(), AddFieldColumn() or AddTimestampColumn() to add columns.
// then Call AddRow() to add rows.
//
// NOTE: column counts MUST match the number of inputs in AddRow()
type Table struct {
	name string

	columnsSchema []*gpb.ColumnSchema
	rows          *gpb.Rows

	// sanitate_needed indicates if sanitate table and column name to snake and lower case
	// Default is true.
	sanitate_needed bool
}

func New(name string) (*Table, error) {
	return &Table{name: name, sanitate_needed: true}, nil
}

func (t *Table) addColumn(name string, semanticType gpb.SemanticType, dataType gpb.ColumnDataType) error {
	name, err := t.sanitate_if_needed(name)
	if err != nil {
		return err
	}

	if t.columnsSchema == nil {
		t.columnsSchema = make([]*gpb.ColumnSchema, 0)
	}

	column := &gpb.ColumnSchema{
		ColumnName:   name,
		SemanticType: semanticType,
		Datatype:     dataType,
	}
	t.columnsSchema = append(t.columnsSchema, column)

	return nil
}

// AddTagColumn helps to add the tag column. You can find details in
// [Data Model].
//
// [Data Model]: https://docs.greptime.com/user-guide/concepts/data-model
func (t *Table) AddTagColumn(name string, type_ types.ColumnType) error {
	typ, err := types.ConvertType(type_)
	if err != nil {
		return err
	}

	return t.addColumn(name, gpb.SemanticType_TAG, typ)
}

// AddFieldColumn helps to add the field column. You can find details in
// [Data Model].
//
// [Data Model]: https://docs.greptime.com/user-guide/concepts/data-model
func (t *Table) AddFieldColumn(name string, type_ types.ColumnType) error {
	typ, err := types.ConvertType(type_)
	if err != nil {
		return err
	}

	return t.addColumn(name, gpb.SemanticType_FIELD, typ)
}

// AddTimestampColumn helps to add the timestamp column. A table can only
// have one timestamp column. You can find details in [Data Model].
//
// [Data Model]: https://docs.greptime.com/user-guide/concepts/data-model
func (t *Table) AddTimestampColumn(name string, type_ types.ColumnType) error {
	typ, err := types.ConvertType(type_)
	if err != nil {
		return err
	}

	return t.addColumn(name, gpb.SemanticType_TIMESTAMP, typ)
}

func (t *Table) addRow(row *gpb.Row) error {
	if t.rows == nil {
		t.rows = &gpb.Rows{}
	}

	if t.rows.Rows == nil {
		t.rows.Rows = make([]*gpb.Row, 0)
	}

	t.rows.Rows = append(t.rows.Rows, row)
	return nil
}

// AddRow is to add real data based on the schema defined before by
// AddTagColumn(), AddFieldColumn() or AddTimestampColumn().
//
// NOTE: The order of inputs MUST match the order of columns in the schema.
//
//		tbl := table.New(<tableName>)
//
//		// add column at first. This is to define the schema of the table.
//		tbl.AddTagColumn("tag1", types.INT64)
//		tbl.AddFieldColumn("field1", types.STRING)
//		tbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MILLISECONDS)
//
//		// you can add multiple row(s). This is the real data.
//		tbl.AddRow(1, "hello", time.Now())
//	    tbl.AddRow(2, "world", time.Now())
func (t *Table) AddRow(inputs ...any) error {
	if t.IsColumnEmpty() {
		return errs.ErrEmptyColumn
	}

	if len(inputs) != len(t.columnsSchema) {
		return fmt.Errorf("number of inputs %d does not match number of columns in schema %d", len(inputs), len(t.columnsSchema))
	}

	row := gpb.Row{
		Values: make([]*gpb.Value, len(inputs)),
	}

	for i, input := range inputs {
		dataType := t.columnsSchema[i].Datatype
		val, err := cell.New(input, dataType).Build()
		if err != nil {
			return err
		}
		row.Values[i] = val
	}
	return t.addRow(&row)
}

func (t *Table) IsColumnEmpty() bool {
	return len(t.columnsSchema) == 0
}

func (t *Table) IsRowEmpty() bool {
	return t.rows == nil || len(t.rows.Rows) == 0
}

func (t *Table) IsEmpty() bool {
	return t.IsColumnEmpty() && t.IsRowEmpty()
}

// WithSanitate to change the sanitate behavior. Default is true.
// sanitate table and column name to snake and lower case.
func (t *Table) WithSanitate(sanitate_needed bool) *Table {
	t.sanitate_needed = sanitate_needed
	return t
}

func (t *Table) WithColumnsSchema(columnsSchema []*gpb.ColumnSchema) *Table {
	t.columnsSchema = columnsSchema
	return t
}

func (t *Table) WithRows(rows *gpb.Rows) *Table {
	t.rows = rows
	return t
}

func (t *Table) GetName() (string, error) {
	return t.sanitate_if_needed(t.name)
}

func (t *Table) GetRows() *gpb.Rows {
	if t.rows != nil && t.rows.Schema == nil {
		t.rows.Schema = t.columnsSchema
	}
	return t.rows
}

func (t *Table) sanitate_if_needed(name string) (string, error) {
	if t.sanitate_needed {
		return util.SanitateName(name)
	}
	return name, nil
}

func (t *Table) ToRequest() (*gpb.RowInsertRequest, error) {
	if t.IsEmpty() {
		return nil, errs.ErrEmptyTable
	}

	name, err := t.GetName()
	if err != nil {
		return nil, err
	}

	req := &gpb.RowInsertRequest{
		TableName: name,
		Rows:      t.GetRows(),
	}
	return req, nil
}
