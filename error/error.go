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

package error

import (
	"errors"
)

var (
	ErrEmptyDatabase        = errors.New("name of database should not be empty")
	ErrEmptyTable           = errors.New("name of table should not be be empty")
	ErrEmptyInserts         = errors.New("at least one insert is required in InsertsRequest")
	ErrEmptyTimestamp       = errors.New("timestamp should not be empty")
	ErrEmptyQuery           = errors.New("query should not be empty, assign Sql, InstantPromql or RangePromql")
	ErrEmptyKey             = errors.New("key should not be empty")
	ErrEmptySql             = errors.New("sql is required in querying")
	ErrEmptyPromql          = errors.New("promql is required in promql querying")
	ErrEmptyStep            = errors.New("step is required in range promql")
	ErrEmptyRange           = errors.New("start and end is required in range promql")
	ErrInvalidTimePrecision = errors.New("precision of timestamp is not valid")
	ErrNoSeriesInMetric     = errors.New("empty series in Metric")
	ErrNotImplemented       = errors.New("not implemented!")
	ErrSqlInPromql          = errors.New("Sql can not be used as Promql")
)
