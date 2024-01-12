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

package greptime

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
	"github.com/GreptimeTeam/greptimedb-ingester-go/model"
)

type monitor struct {
	host        string
	memory      uint64
	cpu         float64
	temperature int64
	ts          time.Time
	isAuthed    bool
}

var (
	database           = "public"
	host               = "127.0.0.1"
	grpcPort, httpPort = 4001, 4000
)

func init() {
	repo := "greptime/greptimedb"
	tag := "v0.4.3"

	var err error
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	log.WithFields(log.Fields{
		"repository": repo,
		"tag":        tag,
	}).Infof("Preparing container %s:%s", repo, tag)

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   repo,
		Tag:          tag,
		ExposedPorts: []string{"4000", "4001", "4002"},
		Entrypoint: []string{"greptime", "standalone", "start",
			"--http-addr=0.0.0.0:4000",
			"--rpc-addr=0.0.0.0:4001",
			"--mysql-addr=0.0.0.0:4002"},
	}, func(config *dc.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = dc.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	var expire uint = 30
	log.WithFields(log.Fields{
		"repository": repo,
		"tag":        tag,
		"expire":     expire,
	}).Infof("Container starting...")

	err = resource.Expire(expire) // Tell docker to hard kill the container
	if err != nil {
		log.WithError(nil).Warn("Expire container failed")
	}

	pool.MaxWait = 30 * time.Second

	if err := pool.Retry(func() error {
		// TODO(vinland-avalon): some functions, like ping() to check if container is ready
		time.Sleep(time.Second)
		httpPort, err = strconv.Atoi(resource.GetPort(("4000/tcp")))
		grpcPort, err = strconv.Atoi(resource.GetPort(("4001/tcp")))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
}

func newClient(t *testing.T) *Client {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := config.New(host).WithPort(grpcPort).WithDatabase(database).WithDialOptions(options...)
	client, err := NewClient(cfg)
	assert.Nil(t, err)
	return client
}

func createTable(t *testing.T, schema string) {
	data := url.Values{}
	data.Set("sql", schema)
	body := strings.NewReader(data.Encode())
	uri := fmt.Sprintf("http://localhost:%d/v1/sql?db=%s", httpPort, database)
	resp, err := http.DefaultClient.Post(uri, "application/x-www-form-urlencoded", body)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	defer resp.Body.Close()
}

func TestInvalidClient(t *testing.T) {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(time.Second),
	}
	cfg := config.New("invalid host").WithPort(grpcPort).WithDatabase(database).WithDialOptions(options...)
	client, err := NewClient(cfg)
	assert.Nil(t, client)
	assert.NotNil(t, err)

	cfg = config.New(host).WithPort(1111).WithDatabase(database).WithDialOptions(options...)
	client, err = NewClient(cfg)
	assert.Nil(t, client)
	assert.NotNil(t, err)
}

func TestInsertAndQueryWithSql(t *testing.T) {

}

func TestPrecisionSecond(t *testing.T) {

}

func TestNilInColumn(t *testing.T) {

}

func TestNoNeedAuth(t *testing.T) {

}

func TestInsertSameColumnWithDifferentType(t *testing.T) {
	table := "insert_same_column_with_different_type"
	client := newClient(t)

	// insert at first
	series := model.Series{}
	series.AddIntTag("count", 1)
	series.SetTimestamp(time.Now())
	metric := model.Metric{}
	metric.AddSeries(series)

	req := InsertRequest{}
	req.WithTable(table).WithMetric(metric)
	reqs := InsertsRequest{}
	reqs.WithDatabase(database).Append(req)
	resp, err := client.Insert(context.Background(), reqs)
	assert.Nil(t, err)
	assert.True(t, ParseRespHeader(resp).IsSuccess())
	assert.False(t, ParseRespHeader(resp).IsRateLimited())
	assert.Equal(t, uint32(1), resp.GetAffectedRows().GetValue())

	// insert again but with different type
	series = model.Series{}
	series.AddFloatTag("count", 1)
	series.SetTimestamp(time.Now())
	metric = model.Metric{}
	metric.AddSeries(series)

	req = InsertRequest{}
	req.WithTable(table).WithMetric(metric)

	reqs = InsertsRequest{}
	reqs.WithDatabase(database).Append(req)
	_, err = client.Insert(context.Background(), reqs)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "reason: column count expect type Int64(Int64Type), given: FLOAT64(10)")
}

func TestInsertTimestampWithDifferentPrecision(t *testing.T) {
	table := "insert_timestamp_with_different_precision"
	client := newClient(t)

	// insert with Second precision at first
	series := model.Series{}
	series.AddIntTag("count", 1)
	series.SetTimestamp(time.Now())
	metric := model.Metric{}
	metric.AddSeries(series)
	metric.SetTimePrecision(time.Second)

	req := InsertRequest{}
	req.WithTable(table).WithMetric(metric)
	reqs := InsertsRequest{}
	reqs.WithDatabase(database).Append(req)
	resp, err := client.Insert(context.Background(), reqs)
	assert.Nil(t, err)
	assert.True(t, ParseRespHeader(resp).IsSuccess())
	assert.False(t, ParseRespHeader(resp).IsRateLimited())
	assert.Equal(t, uint32(1), resp.GetAffectedRows().GetValue())

	// insert again but with different type
	series = model.Series{}
	series.AddIntTag("count", 1)
	series.SetTimestamp(time.Now())
	metric = model.Metric{}
	metric.AddSeries(series)
	metric.SetTimePrecision(time.Millisecond)

	req = InsertRequest{}
	req.WithTable(table).WithMetric(metric)

	reqs = InsertsRequest{}
	reqs.WithDatabase(database).Append(req)
	_, err = client.Insert(context.Background(), reqs)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "reason: column ts expect type Timestamp(Second(TimestampSecondType))")
}

func TestGetNonMatchedTypeColumn(t *testing.T) {

}

func TestGetNotExistColumn(t *testing.T) {

}

func TestDataTypes(t *testing.T) {

}

func TestCreateTableInAdvance(t *testing.T) {

}
