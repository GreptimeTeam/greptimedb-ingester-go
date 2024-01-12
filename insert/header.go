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
	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
	gerr "github.com/GreptimeTeam/greptimedb-ingester-go/error"
	gutil "github.com/GreptimeTeam/greptimedb-ingester-go/util"
)

type reqHeader struct {
	database string
}

func (h *reqHeader) build(cfg *config.Config) (*greptimepb.RequestHeader, error) {
	if gutil.IsEmptyString(h.database) {
		h.database = cfg.Database
	}

	if gutil.IsEmptyString(h.database) {
		return nil, gerr.ErrEmptyDatabase
	}

	header := &greptimepb.RequestHeader{
		Dbname:        h.database,
		Authorization: cfg.BuildAuthHeader(),
	}

	return header, nil
}

type RespHeader struct {
	Code uint32
	Msg  string
}

func (h RespHeader) IsSuccess() bool {
	return h.Code == 0
}

func (h RespHeader) IsRateLimited() bool {
	return h.Code == 6001
}

func (h RespHeader) IsNil() bool {
	return h.Code == 0 && gutil.IsEmptyString(h.Msg)
}

type getRespHeader interface {
	GetHeader() *greptimepb.ResponseHeader
}

func ParseRespHeader[T getRespHeader](r T) RespHeader {
	header := &RespHeader{}
	if r.GetHeader() != nil && r.GetHeader().Status != nil {
		header.Code = r.GetHeader().Status.StatusCode
		header.Msg = r.GetHeader().Status.ErrMsg
	}
	return *header
}
