/*
 * Copyright 2023 Greptime Team
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

package util

import (
	"fmt"
	"strings"

	"github.com/GreptimeTeam/greptimedb-ingester-go/errs"
	"github.com/stoewer/go-strcase"
)

func IsEmptyString(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

// SanitateName will trim leading and trailing spaces at first,
// then convert to lower case and convert spaces to underscores.
func SanitateName(name string) (string, error) {
	if IsEmptyString(name) {
		return "", errs.ErrEmptyName
	}

	s := strings.TrimSpace(name)
	if len(s) >= 100 {
		return "", fmt.Errorf("the length of name CAN NOT be longer than 100. %q", s)
	}

	return strings.ToLower(strcase.SnakeCase(s)), nil
}
