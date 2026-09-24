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
	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

// Same metadata as SQL-created JSON2 columns, so that auto-created tables get
// the JSON2 layout.
const (
	json2ExtensionName     = "greptime.json2"
	json2ExtensionMetadata = `{"json_settings":{"type_hints":[],"max_auto_expanded_paths":100},"layout_version":2}`
)

// MarkJSON2 turns a JSON column schema into a JSON2 one.
func MarkJSON2(col *gpb.ColumnSchema) {
	col.Datatype = gpb.ColumnDataType_JSON
	col.DatatypeExtension = &gpb.ColumnDataTypeExtension{
		TypeExt: &gpb.ColumnDataTypeExtension_JsonNativeType{
			JsonNativeType: &gpb.JsonNativeTypeExtension{Datatype: gpb.ColumnDataType_JSON},
		},
	}
	col.Options = &gpb.ColumnOptions{
		Options: map[string]string{
			"ARROW:extension:name":     json2ExtensionName,
			"ARROW:extension:metadata": json2ExtensionMetadata,
		},
	}
}

// IsJSON2 reports whether the column schema is a JSON2 column.
func IsJSON2(col *gpb.ColumnSchema) bool {
	return col.GetDatatypeExtension().GetJsonNativeType() != nil
}
