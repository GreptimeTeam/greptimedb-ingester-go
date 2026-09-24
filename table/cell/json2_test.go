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

package cell

import (
	"testing"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestBuildJSON2Encoding(t *testing.T) {
	boolean := func(v bool) *gpb.JsonValue { return &gpb.JsonValue{Value: &gpb.JsonValue_Boolean{Boolean: v}} }
	str := func(v string) *gpb.JsonValue { return &gpb.JsonValue{Value: &gpb.JsonValue_Str{Str: v}} }
	uint_ := func(v uint64) *gpb.JsonValue { return &gpb.JsonValue{Value: &gpb.JsonValue_Uint{Uint: v}} }
	int_ := func(v int64) *gpb.JsonValue { return &gpb.JsonValue{Value: &gpb.JsonValue_Int{Int: v}} }
	float := func(v float64) *gpb.JsonValue { return &gpb.JsonValue{Value: &gpb.JsonValue_Float{Float: v}} }
	array := func(items ...*gpb.JsonValue) *gpb.JsonValue {
		return &gpb.JsonValue{Value: &gpb.JsonValue_Array{Array: &gpb.JsonList{Items: items}}}
	}
	object := func(entries ...*gpb.JsonObject_Entry) *gpb.JsonValue {
		return &gpb.JsonValue{Value: &gpb.JsonValue_Object{Object: &gpb.JsonObject{Entries: entries}}}
	}
	entry := func(k string, v *gpb.JsonValue) *gpb.JsonObject_Entry { return &gpb.JsonObject_Entry{Key: k, Value: v} }

	got, err := BuildJSON2(`{"z":{"ok":false},"items":[true,-9223372036854775808,18446744073709551615,18446744073709551616,1.5,-1e3,"你好",null,{},[]]}`)
	require.NoError(t, err)
	want := &gpb.Value{ValueData: &gpb.Value_JsonValue{JsonValue: object(
		entry("items", array(
			boolean(true),
			int_(-9223372036854775808),
			uint_(18446744073709551615),
			float(18446744073709551616),
			float(1.5),
			float(-1000),
			str("你好"),
			&gpb.JsonValue{},
			object(),
			array(),
		)),
		entry("z", object(entry("ok", boolean(false)))),
	)}}
	assert.True(t, proto.Equal(want, got), "got %v", got)

	type payload struct {
		Name string `json:"name"`
		N    int    `json:"n"`
	}
	got, err = BuildJSON2(payload{Name: "a", N: -1})
	require.NoError(t, err)
	want = &gpb.Value{ValueData: &gpb.Value_JsonValue{JsonValue: object(entry("n", int_(-1)), entry("name", str("a")))}}
	assert.True(t, proto.Equal(want, got), "got %v", got)
}

func TestBuildJSON2Null(t *testing.T) {
	nullJSON := "null"
	for _, v := range []any{nil, "null", " null ", &nullJSON, (*string)(nil), (*struct{})(nil), map[string]any(nil)} {
		got, err := BuildJSON2(v)
		require.NoError(t, err, "%#v", v)
		assert.Nil(t, got.GetValueData(), "%#v", v)
	}
}

func TestBuildJSON2Invalid(t *testing.T) {
	for _, v := range []any{
		"",
		"{",
		`{"a":1} trailing`,
		`{"a":1}{}`,
		"[]",
		"1",
		"true",
		`"text"`,
		`{"a":1e400}`,
		[]int{1},
		func() {},
	} {
		_, err := BuildJSON2(v)
		assert.Error(t, err, "%#v", v)
	}
}
