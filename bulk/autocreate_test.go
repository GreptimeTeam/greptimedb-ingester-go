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
	"testing"
	"time"

	gpbv1 "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

func TestAutoCreateColumnConstructors(t *testing.T) {
	tag := TagColumn("host")
	require.NotNil(t, tag.semanticType)
	assert.Equal(t, gpbv1.SemanticType_TAG, *tag.semanticType)

	field := FieldColumn("temperature")
	require.NotNil(t, field.semanticType)
	assert.Equal(t, gpbv1.SemanticType_FIELD, *field.semanticType)

	ts := TimestampColumn("ts")
	require.NotNil(t, ts.semanticType)
	assert.Equal(t, gpbv1.SemanticType_TIMESTAMP, *ts.semanticType)

	plain := AutoCreateColumn{Name: "host", Comment: "a comment"}
	assert.Nil(t, plain.semanticType, "a plain struct literal must not override the semantic type")
}

func TestWithAutoCreateSchemaOption(t *testing.T) {
	bw := &bulkWriter{}
	WithAutoCreateSchema(&AutoCreateSchema{Columns: []AutoCreateColumn{TagColumn("host")}})(bw)
	require.NotNil(t, bw.autoCreate)
	require.Len(t, bw.autoCreate.Columns, 1)
}

func TestApplyAutoCreateMetadata(t *testing.T) {
	rows := testRows(t, true)
	record, err := types.NewArrowConverter().ToArrow(rows)
	require.NoError(t, err)
	defer record.Release()

	schema := &AutoCreateSchema{
		Columns: []AutoCreateColumn{
			TagColumn("host"),
			{Name: "temperature", Comment: "sensor temperature"},
			TimestampColumn("ts"),
			FieldColumn("payload"),
		},
	}
	enriched, err := applyAutoCreateMetadata(record, rows.Schema, schema)
	require.NoError(t, err)
	defer enriched.Release()

	require.Equal(t, record.NumRows(), enriched.NumRows())
	require.Equal(t, record.NumCols(), enriched.NumCols())

	field := enriched.Schema().Field(0)
	assert.Equal(t, "host", field.Name)
	assert.True(t, field.Nullable)
	assertMetadata(t, field, semanticTypeMetadataKey, "tag")

	field = enriched.Schema().Field(1)
	assert.Equal(t, "temperature", field.Name)
	assert.True(t, field.Nullable)
	assertMetadata(t, field, semanticTypeMetadataKey, "field")
	assertMetadata(t, field, commentMetadataKey, "sensor temperature")

	field = enriched.Schema().Field(2)
	assert.Equal(t, "ts", field.Name)
	assert.False(t, field.Nullable, "the timestamp column must be non-nullable")
	assertMetadata(t, field, semanticTypeMetadataKey, "timestamp")

	field = enriched.Schema().Field(3)
	assert.Equal(t, "payload", field.Name)
	assert.True(t, field.Nullable)
	assertMetadata(t, field, semanticTypeMetadataKey, "field")
	assertMetadata(t, field, typeMetadataKey, "Json")
}

func TestApplyAutoCreateMetadataFallsBackToTableSchema(t *testing.T) {
	rows := testRows(t, false)
	record, err := types.NewArrowConverter().ToArrow(rows)
	require.NoError(t, err)
	defer record.Release()

	// Only a comment is provided for one column; semantic types come from the
	// table's own schema, including the JSON type override.
	schema := &AutoCreateSchema{
		Columns: []AutoCreateColumn{{Name: "temperature", Comment: "sensor temperature"}},
	}
	enriched, err := applyAutoCreateMetadata(record, rows.Schema, schema)
	require.NoError(t, err)
	defer enriched.Release()

	assertMetadata(t, enriched.Schema().Field(0), semanticTypeMetadataKey, "tag")
	assertMetadata(t, enriched.Schema().Field(1), semanticTypeMetadataKey, "field")
	assertMetadata(t, enriched.Schema().Field(1), commentMetadataKey, "sensor temperature")
	assertMetadata(t, enriched.Schema().Field(2), semanticTypeMetadataKey, "timestamp")
	assert.False(t, enriched.Schema().Field(2).Nullable)
}

func TestApplyAutoCreateMetadataRejectsInvalidSchemas(t *testing.T) {
	rows := testRows(t, false)
	record, err := types.NewArrowConverter().ToArrow(rows)
	require.NoError(t, err)
	defer record.Release()

	t.Run("empty schema", func(t *testing.T) {
		_, err := applyAutoCreateMetadata(record, rows.Schema, &AutoCreateSchema{})
		require.Error(t, err)
	})

	t.Run("duplicate column", func(t *testing.T) {
		_, err := applyAutoCreateMetadata(record, rows.Schema, &AutoCreateSchema{
			Columns: []AutoCreateColumn{TagColumn("host"), TagColumn("host")},
		})
		require.Error(t, err)
	})

	t.Run("type override on timestamp", func(t *testing.T) {
		_, err := applyAutoCreateMetadata(record, rows.Schema, &AutoCreateSchema{
			Columns: []AutoCreateColumn{{Name: "ts", TypeOverride: "Json"}},
		})
		require.Error(t, err)
	})

	t.Run("spec column not in table", func(t *testing.T) {
		_, err := applyAutoCreateMetadata(record, rows.Schema, &AutoCreateSchema{
			Columns: []AutoCreateColumn{TagColumn("hotst")},
		})
		require.ErrorContains(t, err, `[hotst]`)
	})

	t.Run("ambiguous after sanitization", func(t *testing.T) {
		_, err := applyAutoCreateMetadata(record, rows.Schema, &AutoCreateSchema{
			Columns: []AutoCreateColumn{TagColumn("MyHost"), TagColumn("my_host")},
		})
		require.ErrorContains(t, err, "ambiguous")
	})
}

// A spec column that differs from the written field only by sanitization still
// matches, so both sanitized and unsanitized tables are supported.
func TestApplyAutoCreateMetadataMatchesBySanitizedName(t *testing.T) {
	rows := testRows(t, false)
	record, err := types.NewArrowConverter().ToArrow(rows)
	require.NoError(t, err)
	defer record.Release()

	schema := &AutoCreateSchema{
		Columns: []AutoCreateColumn{{Name: "HOST", Comment: "source host"}},
	}
	enriched, err := applyAutoCreateMetadata(record, rows.Schema, schema)
	require.NoError(t, err)
	defer enriched.Release()

	assertMetadata(t, enriched.Schema().Field(0), commentMetadataKey, "source host")
}

// With a table built via WithSanitate(false), spec columns match by exact
// name, so their overrides are not silently dropped.
func TestApplyAutoCreateMetadataMatchesUnsanitizedTable(t *testing.T) {
	tb, err := table.New("test_table")
	require.NoError(t, err)
	tb.WithSanitate(false)
	require.NoError(t, tb.AddFieldColumn("MyHost", types.STRING))
	require.NoError(t, tb.AddTimestampColumn("Ts", types.TIMESTAMP_MICROSECOND))
	require.NoError(t, tb.AddRow("host-1", time.Unix(0, 0).UnixMicro()))
	rows := tb.GetRows()

	record, err := types.NewArrowConverter().ToArrow(rows)
	require.NoError(t, err)
	defer record.Release()

	schema := &AutoCreateSchema{
		Columns: []AutoCreateColumn{TagColumn("MyHost")},
	}
	enriched, err := applyAutoCreateMetadata(record, rows.Schema, schema)
	require.NoError(t, err)
	defer enriched.Release()

	// The semantic type override must win over the table's FIELD declaration.
	assertMetadata(t, enriched.Schema().Field(0), semanticTypeMetadataKey, "tag")
}

func testRows(t *testing.T, withJSON bool) *gpbv1.Rows {
	t.Helper()
	tb, err := table.New("test_table")
	require.NoError(t, err)
	require.NoError(t, tb.AddTagColumn("host", types.STRING))
	require.NoError(t, tb.AddFieldColumn("temperature", types.FLOAT64))
	require.NoError(t, tb.AddTimestampColumn("ts", types.TIMESTAMP_MICROSECOND))
	if withJSON {
		require.NoError(t, tb.AddFieldColumn("payload", types.JSON))
	}

	now := time.Unix(0, 0).UnixMicro()
	if withJSON {
		require.NoError(t, tb.AddRow("host-1", 20.5, now, `{"a":1}`))
		require.NoError(t, tb.AddRow("host-1", 20.5, now, `{"b":2}`))
	} else {
		require.NoError(t, tb.AddRow("host-1", 20.5, now))
	}
	return tb.GetRows()
}

func assertMetadata(t *testing.T, field arrow.Field, key, want string) {
	t.Helper()
	got, ok := field.Metadata.GetValue(key)
	require.True(t, ok, "field %q must carry metadata %q", field.Name, key)
	assert.Equal(t, want, got)
}
