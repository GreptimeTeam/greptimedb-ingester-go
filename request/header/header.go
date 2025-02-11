/*
 *    Copyright 2024 Greptime Team
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package header

import (
	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	"github.com/GreptimeTeam/greptimedb-ingester-go/errs"
	"github.com/GreptimeTeam/greptimedb-ingester-go/util"
)

type Header struct {
	database string
	auth     Auth
}

func New(database string) *Header {
	return &Header{
		database: database,
	}
}

func (h *Header) WithDatabase(database string) *Header {
	h.database = database
	return h
}

func (h *Header) WithAuth(username, password string) *Header {
	h.auth = newAuth(username, password)
	return h
}

func (h *Header) Build() (*gpb.RequestHeader, error) {
	if util.IsEmptyString(h.database) {
		return nil, errs.ErrEmptyDatabaseName
	}

	header := &gpb.RequestHeader{
		Dbname:        h.database,
		Authorization: h.auth.buildAuthHeader(),
	}

	return header, nil
}
