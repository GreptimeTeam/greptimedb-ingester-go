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

package insert

import (
	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
	"github.com/GreptimeTeam/greptimedb-ingester-go/errs"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
)

type RowInsertsRequest struct {
	header reqHeader
	tables []*table.Table
}

func NewRowInsertsRequest(tables ...*table.Table) *RowInsertsRequest {
	return &RowInsertsRequest{
		tables: tables,
	}
}

func (r *RowInsertsRequest) IsTablesEmpty() bool {
	return r.tables == nil || len(r.tables) == 0
}

func (r *RowInsertsRequest) WithDatabase(database string) *RowInsertsRequest {
	r.header = reqHeader{
		database: database,
	}
	return r
}

func (r *RowInsertsRequest) AddTable(tables ...*table.Table) *RowInsertsRequest {
	if r.tables == nil {
		r.tables = make([]*table.Table, 0)
	}

	r.tables = append(r.tables, tables...)
	return r
}

func (r *RowInsertsRequest) Build(cfg *config.Config) (*gpb.GreptimeRequest, error) {
	header, err := r.header.build(cfg)
	if err != nil {
		return nil, err
	}

	if r.IsTablesEmpty() {
		return nil, errs.ErrEmptyTables
	}

	reqs := make([]*gpb.RowInsertRequest, 0, len(r.tables))
	for _, tbl := range r.tables {
		req := &gpb.RowInsertRequest{
			TableName: tbl.Schema.Name,
			Rows:      tbl.Rows,
		}
		reqs = append(reqs, req)
	}

	req := gpb.GreptimeRequest_RowInserts{
		RowInserts: &gpb.RowInsertRequests{Inserts: reqs},
	}

	return &gpb.GreptimeRequest{
		Header:  header,
		Request: &req,
	}, nil

}
