// Copyright 2024 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMetric(t *testing.T) {
	s := Series{}
	assert.Nil(t, s.AddTag("tag1", "tag val"))
	assert.Nil(t, s.AddTag("tag2", true))
	assert.Nil(t, s.AddTag("tag3", int32(32)))
	assert.Nil(t, s.AddTag("tag4", float64(32.0)))
	assert.Nil(t, s.AddField("field1", []byte("field val")))
	assert.Nil(t, s.AddField("field2", float32(32.0)))
	assert.Nil(t, s.AddField("field3", uint8(8)))
	assert.Nil(t, s.AddField("field4", uint64(64)))
	assert.Nil(t, s.SetTimestamp(time.Now()))

	m := Metric{}
	err := m.AddSeries(s)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(m.GetSeries()))
}

func TestMetricTypeNotMatch(t *testing.T) {
	s1 := Series{}
	assert.Nil(t, s1.AddTag("tag", "tag val"))
	assert.Nil(t, s1.SetTimestamp(time.Now()))

	s2 := Series{}
	assert.Nil(t, s2.AddTag("tag", true))
	assert.Nil(t, s2.SetTimestamp(time.Now()))

	m := Metric{}
	assert.Nil(t, m.AddSeries(s1))
	assert.NotNil(t, m.AddSeries(s2))
}

func TestMetricSemanticNotMatch(t *testing.T) {
	s1 := Series{}
	assert.Nil(t, s1.AddTag("name", "tag val"))
	assert.Nil(t, s1.SetTimestamp(time.Now()))

	s2 := Series{}
	assert.Nil(t, s2.AddField("name", true))
	assert.Nil(t, s2.SetTimestamp(time.Now()))

	m := Metric{}
	assert.Nil(t, m.AddSeries(s1))
	assert.NotNil(t, m.AddSeries(s2))
}
