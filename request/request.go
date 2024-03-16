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

	"github.com/GreptimeTeam/greptimedb-ingester-go/errs"
	"github.com/GreptimeTeam/greptimedb-ingester-go/request/header"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

type Request struct {
	header *header.Header
	tables []*table.Table
}

func New(header *header.Header, tables ...*table.Table) *Request {
	return &Request{
		header: header,
		tables: tables,
	}
}

func (r *Request) WithHeader(header *header.Header) *Request {
	r.header = header
	return r
}

func (r *Request) WithTables(tables ...*table.Table) *Request {
	if r.tables == nil {
		r.tables = make([]*table.Table, 0)
	}

	r.tables = append(r.tables, tables...)
	return r
}

func (r *Request) IsZero() bool {
	return r.tables == nil || len(r.tables) == 0
}

func (r *Request) Build(writeOp types.WriteOp) (*gpb.GreptimeRequest, error) {
	if r.IsZero() {
		return nil, errs.ErrEmptyTable
	}

	header, err := r.header.Build()
	if err != nil {
		return nil, err
	}

	switch writeOp {
	case types.Insert:
		insertReqs := make([]*gpb.RowInsertRequest, 0, len(r.tables))
		for _, table := range r.tables {
			req, err := table.ToInsertRequest()
			if err != nil {
				return nil, err
			}
			insertReqs = append(insertReqs, req)
		}
		req := &gpb.GreptimeRequest_RowInserts{
			RowInserts: &gpb.RowInsertRequests{Inserts: insertReqs},
		}
		return &gpb.GreptimeRequest{
			Header:  header,
			Request: req,
		}, nil
	case types.Delete:
		deleteReqs := make([]*gpb.RowDeleteRequest, 0, len(r.tables))
		for _, table := range r.tables {
			req, err := table.ToDeleteRequest()
			if err != nil {
				return nil, err
			}
			deleteReqs = append(deleteReqs, req)
		}

		req := &gpb.GreptimeRequest_RowDeletes{
			RowDeletes: &gpb.RowDeleteRequests{Deletes: deleteReqs},
		}
		return &gpb.GreptimeRequest{
			Header:  header,
			Request: req,
		}, nil
	}
	return nil, errs.ErrInvalidWriteOp
}
