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

package types

import (
	"testing"
	"time"

	gpbv1 "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DATETIME is a deprecated alias for TIMESTAMP_MICROSECOND (greptimedb PR #5506).
// Bulk (Arrow) and unary paths must agree on microsecond precision; previously
// bulk mapped DATETIME to Timestamp_ms and had no fillValue case for the
// microsecond timestamp value, so bulk writes of a DATETIME column failed with
// a propagated conversion error.
func TestConvertToArrowType_DatetimeIsMicrosecond(t *testing.T) {
	c := NewArrowConverter()

	dtType, err := c.convertToArrowType(gpbv1.ColumnDataType_DATETIME)
	require.NoError(t, err)
	assert.Equal(t, timestampTypeUs, dtType)

	tsType, err := c.convertToArrowType(gpbv1.ColumnDataType_TIMESTAMP_MICROSECOND)
	require.NoError(t, err)
	assert.Equal(t, dtType, tsType, "DATETIME must map to the same Arrow type as TIMESTAMP_MICROSECOND")
}

func TestToArrow_DatetimeColumn(t *testing.T) {
	now := time.Date(2026, 4, 21, 10, 30, 45, 123456000, time.UTC)
	micros := now.UnixMicro()

	rows := &gpbv1.Rows{
		Schema: []*gpbv1.ColumnSchema{
			{
				ColumnName:   "ts",
				SemanticType: gpbv1.SemanticType_TIMESTAMP,
				Datatype:     gpbv1.ColumnDataType_DATETIME,
			},
		},
		Rows: []*gpbv1.Row{
			{
				Values: []*gpbv1.Value{
					{ValueData: &gpbv1.Value_TimestampMicrosecondValue{TimestampMicrosecondValue: micros}},
				},
			},
			{
				Values: []*gpbv1.Value{nil},
			},
		},
	}

	record, err := NewArrowConverter().ToArrow(rows)
	require.NoError(t, err)
	defer record.Release()

	require.Equal(t, int64(2), record.NumRows())
	require.Equal(t, int64(1), record.NumCols())

	field := record.Schema().Field(0)
	assert.Equal(t, timestampTypeUs, field.Type)

	col, ok := record.Column(0).(*array.Timestamp)
	require.True(t, ok, "column must be *array.Timestamp")
	assert.Equal(t, arrow.Timestamp(micros), col.Value(0))
	assert.True(t, col.IsNull(1))
}
