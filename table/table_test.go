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

package table

import (
	"testing"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

func TestJSON2Column(t *testing.T) {
	tbl, err := New("json2_logs")
	require.NoError(t, err)
	assert.Error(t, tbl.AddTagColumn("tag", types.JSON2))
	assert.Error(t, tbl.AddTimestampColumn("ts", types.JSON2))

	require.NoError(t, tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND))
	require.NoError(t, tbl.AddFieldColumn("payload", types.JSON2))
	require.NoError(t, tbl.AddFieldColumn("legacy", types.JSON))
	require.NoError(t, tbl.AddRow(1, `{"a":1}`, `{"a":1}`))
	require.NoError(t, tbl.AddRow(2, nil, nil))
	assert.Error(t, tbl.AddRow(3, `[1]`, `{}`))

	req, err := tbl.ToInsertRequest()
	require.NoError(t, err)
	schema := req.Rows.Schema
	assert.Equal(t, gpb.ColumnDataType_JSON, schema[1].Datatype)
	assert.Equal(t, gpb.ColumnDataType_JSON, schema[1].GetDatatypeExtension().GetJsonNativeType().GetDatatype())
	assert.Equal(t, "greptime.json2", schema[1].GetOptions().GetOptions()["ARROW:extension:name"])
	assert.False(t, types.IsJSON2(schema[2]))

	rows := req.Rows.Rows
	require.Len(t, rows, 2)
	assert.Equal(t, uint64(1), rows[0].Values[1].GetJsonValue().GetObject().GetEntries()[0].GetValue().GetUint())
	assert.Equal(t, `{"a":1}`, rows[0].Values[2].GetStringValue())
	assert.Nil(t, rows[1].Values[1].GetValueData())
}

func TestBulkConversionPreservesNullFields(t *testing.T) {
	tbl, err := New("sensor_readings")
	require.NoError(t, err)
	require.NoError(t, tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND))
	require.NoError(t, tbl.AddFieldColumn("reading", types.INT64))
	require.NoError(t, tbl.AddFieldColumn("label", types.STRING))
	require.NoError(t, tbl.AddFieldColumn("active", types.BOOLEAN))
	require.NoError(t, tbl.AddRow(int64(1), nil, nil, nil))
	require.NoError(t, tbl.AddRow(int64(2), int64(0), "", false))
	require.NoError(t, tbl.AddRow(int64(3), int64(42), "sensor-a", true))

	record, err := types.NewArrowConverter().ToArrow(tbl.GetRows())
	require.NoError(t, err)
	defer record.Release()

	for _, column := range record.Columns()[1:] {
		assert.True(t, column.IsNull(0), "missing value in %s", column.DataType())
		assert.False(t, column.IsNull(1), "explicit zero in %s", column.DataType())
		assert.False(t, column.IsNull(2), "ordinary value in %s", column.DataType())
	}
	readings := record.Column(1).(*array.Int64)
	labels := record.Column(2).(*array.String)
	activity := record.Column(3).(*array.Boolean)
	assert.Equal(t, int64(0), readings.Value(1))
	assert.Empty(t, labels.Value(1))
	assert.False(t, activity.Value(1))
	assert.Equal(t, int64(42), readings.Value(2))
	assert.Equal(t, "sensor-a", labels.Value(2))
	assert.True(t, activity.Value(2))
}
