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

package errs

import (
	"errors"
)

var (
	ErrEmptyName            = errors.New("name should not be be empty")
	ErrEmptyDatabaseName    = errors.New("name of database should not be empty")
	ErrEmptyTableName       = errors.New("name of table should not be be empty")
	ErrEmptyTables          = errors.New("please add at least one record before sending insert request")
	ErrEmptyTimestamp       = errors.New("timestamp should not be empty")
	ErrInvalidTimePrecision = errors.New("precision of timestamp is not valid")

	ErrColumnNotSet = errors.New("column not set, please call AddColumn first")
)
