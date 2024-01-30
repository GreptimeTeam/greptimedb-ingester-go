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

package request

import (
	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
	"github.com/GreptimeTeam/greptimedb-ingester-go/errs"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
)

type Request struct {
	header reqHeader
	tables []*table.Table
}

func New(tables ...*table.Table) *Request {
	return &Request{
		tables: tables,
	}
}

func (r *Request) IsTablesEmpty() bool {
	return r.tables == nil || len(r.tables) == 0
}

func (r *Request) WithDatabase(database string) *Request {
	r.header = reqHeader{
		database: database,
	}
	return r
}

func (r *Request) WithTables(tables ...*table.Table) *Request {
	if r.tables == nil {
		r.tables = make([]*table.Table, 0)
	}

	r.tables = append(r.tables, tables...)
	return r
}

func (r *Request) Build(cfg *config.Config) (*gpb.GreptimeRequest, error) {
	if r.IsTablesEmpty() {
		return nil, errs.ErrEmptyTable
	}

	header, err := r.header.build(cfg)
	if err != nil {
		return nil, err
	}

	reqs := make([]*gpb.RowInsertRequest, 0, len(r.tables))
	for _, table := range r.tables {
		req, err := table.ToRequest()
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, req)
	}

	req := &gpb.GreptimeRequest_RowInserts{
		RowInserts: &gpb.RowInsertRequests{Inserts: reqs},
	}

	return &gpb.GreptimeRequest{Header: header, Request: req}, nil

}
