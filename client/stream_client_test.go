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

package client

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
	tbl "github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

var (
	streamClient *StreamClient
)

func newStreamClient() *StreamClient {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := config.New(host).
		WithPort(grpcPort).
		WithDatabase(database).
		WithDialOptions(options...)

	client, err := NewStreamClient(cfg)
	if err != nil {
		log.Fatalf("failed to create client: %s", err.Error())
	}
	return client
}

func TestStreamInsert(t *testing.T) {
	loc, err := time.LoadLocation(timezone)
	assert.Nil(t, err)
	ts1 := time.Now().Add(-1 * time.Minute).UnixMilli()
	time1 := time.UnixMilli(ts1).In(loc)
	ts2 := time.Now().Add(-2 * time.Minute).UnixMilli()
	time2 := time.UnixMilli(ts2).In(loc)

	monitors := []monitor{
		{
			ID:          randomId(),
			Host:        "127.0.0.1",
			Memory:      1,
			Cpu:         1.0,
			Temperature: -1,
			Ts:          time1,
			Running:     true,
		},
		{
			ID:          randomId(),
			Host:        "127.0.0.2",
			Memory:      2,
			Cpu:         2.0,
			Temperature: -2,
			Ts:          time2,
			Running:     true,
		},
	}

	table, err := tbl.New(monitorTableName)
	assert.Nil(t, err)

	assert.Nil(t, table.AddTagColumn("id", types.INT64))
	assert.Nil(t, table.AddTagColumn("host", types.STRING))
	assert.Nil(t, table.AddFieldColumn("memory", types.UINT64))
	assert.Nil(t, table.AddFieldColumn("cpu", types.FLOAT64))
	assert.Nil(t, table.AddFieldColumn("temperature", types.INT64))
	assert.Nil(t, table.AddFieldColumn("running", types.BOOLEAN))
	assert.Nil(t, table.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND))

	for _, monitor := range monitors {
		err := table.AddRow(monitor.ID, monitor.Host,
			monitor.Memory, monitor.Cpu, monitor.Temperature, monitor.Running,
			monitor.Ts)
		assert.Nil(t, err)
	}

	err = streamClient.Send(context.Background(), table)
	assert.Nil(t, err)
	affected, err := streamClient.CloseAndRecv(context.Background())
	assert.EqualValues(t, 2, affected.GetValue())
	assert.Nil(t, err)

	monitors_, err := db.Query(fmt.Sprintf("select * from %s where id in %s order by host asc", monitorTableName, getMonitorsIds(monitors)))
	assert.Nil(t, err)

	assert.Equal(t, len(monitors), len(monitors_))

	for i, monitor_ := range monitors_ {
		assert.Equal(t, monitors[i], monitor_)
	}
}
