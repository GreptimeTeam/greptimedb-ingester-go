/*
 * Copyright 2026 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bulk

import (
	"fmt"
	"maps"
	"slices"

	gpbv1 "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/GreptimeTeam/greptimedb-ingester-go/util"
)

// Arrow field metadata keys recognized by GreptimeDB Enterprise when
// auto-creating a missing table for a Flight bulk insert.
const (
	// semanticTypeMetadataKey carries the semantic type of a column. The value
	// must be "tag", "field" or "timestamp"; exactly one "timestamp" column
	// becomes the time index of the created table.
	semanticTypeMetadataKey = "greptime:semantic_type"
	// typeMetadataKey carries the GreptimeDB extended data type of a column,
	// e.g. "Json" for a JSON column.
	typeMetadataKey = "greptime:type"
	// commentMetadataKey carries the column comment.
	commentMetadataKey = "greptime:comment"
)

// AutoCreateColumn describes the schema metadata of a single column needed by
// GreptimeDB Enterprise to auto-create a missing table during a Flight bulk
// insert. The metadata is attached to the Arrow schema of every written record
// batch and matched to columns by (sanitized) name.
//
// The semantic type defaults to the one declared by the written table's schema.
// Use TagColumn, FieldColumn or TimestampColumn to override it, and a plain
// struct literal to only attach a comment or a type override.
type AutoCreateColumn struct {
	// Name is the column name. It matches a written column by exact name first,
	// then by sanitized name, so it works with both sanitized and unsanitized
	// tables (see table.Table.WithSanitate).
	Name string
	// Comment is an optional column comment, carried as greptime:comment.
	Comment string
	// TypeOverride is an optional GreptimeDB extended data type, carried as
	// greptime:type, for example "Json". JSON columns are auto-detected from
	// the written table's schema, so it only needs to be set for columns whose
	// extended type cannot be inferred from the Arrow type.
	TypeOverride string

	// semanticType overrides the semantic type declared by the written table's
	// schema. It is only set through TagColumn, FieldColumn or TimestampColumn;
	// nil falls back to the table's own schema.
	semanticType *gpbv1.SemanticType
}

// TagColumn returns an AutoCreateColumn with the TAG semantic type. Tag
// columns become the primary key of the created table, in schema order.
func TagColumn(name string) AutoCreateColumn {
	return AutoCreateColumn{Name: name, semanticType: ptrOf(gpbv1.SemanticType_TAG)}
}

// FieldColumn returns an AutoCreateColumn with the FIELD semantic type.
func FieldColumn(name string) AutoCreateColumn {
	return AutoCreateColumn{Name: name, semanticType: ptrOf(gpbv1.SemanticType_FIELD)}
}

// TimestampColumn returns an AutoCreateColumn with the TIMESTAMP semantic
// type. The timestamp column is the time index of the created table and is
// written as non-nullable.
func TimestampColumn(name string) AutoCreateColumn {
	return AutoCreateColumn{Name: name, semanticType: ptrOf(gpbv1.SemanticType_TIMESTAMP)}
}

func ptrOf(t gpbv1.SemanticType) *gpbv1.SemanticType {
	return &t
}

// AutoCreateSchema carries the column metadata that GreptimeDB Enterprise
// needs to auto-create a missing table for a Flight bulk insert.
type AutoCreateSchema struct {
	Columns []AutoCreateColumn
}

// WithAutoCreateSchema configures the bulk writer to attach the given column
// metadata to the Arrow schema of every written record batch, so that
// GreptimeDB Enterprise can auto-create the table if it does not exist yet.
func WithAutoCreateSchema(schema *AutoCreateSchema) WriterOption {
	return func(bw *bulkWriter) {
		bw.autoCreate = schema
	}
}

// applyAutoCreateMetadata returns a copy of rec whose schema carries the
// auto-create metadata for each column. Metadata is matched to columns by
// name: an exact match on the written field name wins, falling back to the
// sanitized name so spec columns work for both sanitized and unsanitized
// tables. Columns present in the schema override the written table's own
// column schema, and the table's schema fills in every remaining column.
// JSON columns are tagged greptime:type=Json so the server can distinguish
// them from binary columns when creating the table. A schema column that
// matches nothing in the written table is an error.
func applyAutoCreateMetadata(
	rec arrow.Record,
	columns []*gpbv1.ColumnSchema,
	schema *AutoCreateSchema,
) (arrow.Record, error) {
	if schema == nil || len(schema.Columns) == 0 {
		return nil, fmt.Errorf("auto-create schema is empty")
	}

	exactByName := make(map[string]AutoCreateColumn, len(schema.Columns))
	sanitizedByName := make(map[string]string, len(schema.Columns))
	for _, col := range schema.Columns {
		name, err := util.SanitateName(col.Name)
		if err != nil {
			return nil, fmt.Errorf("invalid auto-create column name %q: %w", col.Name, err)
		}
		if _, dup := exactByName[col.Name]; dup {
			return nil, fmt.Errorf("duplicate column %q in auto-create schema", col.Name)
		}
		if other, dup := sanitizedByName[name]; dup && other != col.Name {
			return nil, fmt.Errorf(
				"auto-create schema columns %q and %q are ambiguous after sanitization",
				other, col.Name,
			)
		}
		exactByName[col.Name] = col
		sanitizedByName[name] = col.Name
	}

	lookup := func(fieldName string) (AutoCreateColumn, bool) {
		if col, ok := exactByName[fieldName]; ok {
			return col, true
		}
		if sanitized, err := util.SanitateName(fieldName); err == nil {
			if raw, ok := sanitizedByName[sanitized]; ok {
				return exactByName[raw], true
			}
		}
		return AutoCreateColumn{}, false
	}
	consume := func(col AutoCreateColumn) {
		delete(exactByName, col.Name)
		if sanitized, err := util.SanitateName(col.Name); err == nil {
			delete(sanitizedByName, sanitized)
		}
	}

	tableByName := make(map[string]*gpbv1.ColumnSchema, len(columns))
	for _, col := range columns {
		tableByName[col.ColumnName] = col
	}

	fields := rec.Schema().Fields()
	enriched := make([]arrow.Field, len(fields))
	for i, field := range fields {
		enriched[i] = field
		specCol, inSpec := lookup(field.Name)
		if inSpec {
			consume(specCol)
		}
		tableCol, inTable := tableByName[field.Name]

		semanticType, ok := columnSemanticType(specCol, inSpec, tableCol)
		if !ok {
			continue
		}
		if semanticType == gpbv1.SemanticType_TIMESTAMP && specCol.TypeOverride != "" {
			return nil, fmt.Errorf(
				"column %q is a timestamp and cannot carry a type override",
				field.Name,
			)
		}

		metadata := map[string]string{
			semanticTypeMetadataKey: semanticTypeString(semanticType),
		}
		if specCol.TypeOverride != "" {
			metadata[typeMetadataKey] = specCol.TypeOverride
		} else if inTable && tableCol.Datatype == gpbv1.ColumnDataType_JSON {
			metadata[typeMetadataKey] = "Json"
		}
		if specCol.Comment != "" {
			metadata[commentMetadataKey] = specCol.Comment
		}

		nullable := field.Nullable
		if semanticType == gpbv1.SemanticType_TIMESTAMP {
			nullable = false
		}
		enriched[i] = arrow.Field{
			Name:     field.Name,
			Type:     field.Type,
			Nullable: nullable,
			Metadata: arrow.MetadataFrom(metadata),
		}
	}

	if len(exactByName) > 0 {
		return nil, fmt.Errorf(
			"auto-create schema columns %v not found in the written table",
			slices.Sorted(maps.Keys(exactByName)),
		)
	}

	metadata := rec.Schema().Metadata()
	return array.NewRecord(arrow.NewSchema(enriched, &metadata), rec.Columns(), rec.NumRows()), nil
}

func columnSemanticType(
	specCol AutoCreateColumn,
	inSpec bool,
	tableCol *gpbv1.ColumnSchema,
) (gpbv1.SemanticType, bool) {
	if inSpec && specCol.semanticType != nil {
		return *specCol.semanticType, true
	}
	if tableCol != nil {
		return tableCol.SemanticType, true
	}
	return gpbv1.SemanticType_TAG, false
}

func semanticTypeString(t gpbv1.SemanticType) string {
	switch t {
	case gpbv1.SemanticType_TAG:
		return "tag"
	case gpbv1.SemanticType_FIELD:
		return "field"
	case gpbv1.SemanticType_TIMESTAMP:
		return "timestamp"
	default:
		return ""
	}
}
