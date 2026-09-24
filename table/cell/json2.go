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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

// BuildJSON2 encodes v as a native JSON2 value. string and *string are parsed
// as JSON text; other values are marshaled with encoding/json first.
//
// JSON null becomes SQL NULL. Any other top-level value must be a JSON object;
// nested arrays and scalars are allowed.
func BuildJSON2(v any) (*gpb.Value, error) {
	var data []byte
	switch t := v.(type) {
	case nil:
		return &gpb.Value{}, nil
	case string:
		data = []byte(t)
	case *string:
		if t == nil {
			return &gpb.Value{}, nil
		}
		data = []byte(*t)
	default:
		var err error
		if data, err = json.Marshal(v); err != nil {
			return nil, fmt.Errorf("invalid JSON2 value: %w", err)
		}
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	// Keep numbers as text so 64-bit integers are not rounded through float64.
	dec.UseNumber()
	var parsed any
	if err := dec.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("invalid JSON2 value: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("invalid JSON2 value: unexpected data after the top-level value")
	}

	switch parsed.(type) {
	case nil:
		return &gpb.Value{}, nil
	case map[string]any:
	default:
		return nil, errors.New("invalid JSON2 value: expected a JSON object or null")
	}

	jsonValue, err := encodeJSON2(parsed)
	if err != nil {
		return nil, err
	}
	return &gpb.Value{ValueData: &gpb.Value_JsonValue{JsonValue: jsonValue}}, nil
}

func encodeJSON2(v any) (*gpb.JsonValue, error) {
	switch t := v.(type) {
	case nil:
		return &gpb.JsonValue{}, nil
	case bool:
		return &gpb.JsonValue{Value: &gpb.JsonValue_Boolean{Boolean: t}}, nil
	case string:
		return &gpb.JsonValue{Value: &gpb.JsonValue_Str{Str: t}}, nil
	case json.Number:
		return encodeJSON2Number(t)
	case []any:
		items := make([]*gpb.JsonValue, len(t))
		for i, item := range t {
			encoded, err := encodeJSON2(item)
			if err != nil {
				return nil, err
			}
			items[i] = encoded
		}
		return &gpb.JsonValue{Value: &gpb.JsonValue_Array{Array: &gpb.JsonList{Items: items}}}, nil
	case map[string]any:
		// Sort keys so the encoded request does not depend on map iteration order.
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		entries := make([]*gpb.JsonObject_Entry, len(keys))
		for i, k := range keys {
			encoded, err := encodeJSON2(t[k])
			if err != nil {
				return nil, err
			}
			entries[i] = &gpb.JsonObject_Entry{Key: k, Value: encoded}
		}
		return &gpb.JsonValue{Value: &gpb.JsonValue_Object{Object: &gpb.JsonObject{Entries: entries}}}, nil
	default:
		return nil, fmt.Errorf("invalid JSON2 value: unexpected decoded type %T", v)
	}
}

// Integers use uint64 when non-negative, int64 when negative, and fall back to
// float64 when they do not fit in 64 bits.
func encodeJSON2Number(n json.Number) (*gpb.JsonValue, error) {
	s := n.String()
	if !strings.ContainsAny(s, ".eE") {
		if u, err := strconv.ParseUint(s, 10, 64); err == nil {
			return &gpb.JsonValue{Value: &gpb.JsonValue_Uint{Uint: u}}, nil
		}
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return &gpb.JsonValue{Value: &gpb.JsonValue_Int{Int: i}}, nil
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid JSON2 value: number %s is out of range", s)
	}
	return &gpb.JsonValue{Value: &gpb.JsonValue_Float{Float: f}}, nil
}
