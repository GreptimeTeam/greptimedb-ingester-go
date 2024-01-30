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
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
	tbl "github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

var (
	database                      = "public"
	host                          = "127.0.0.1"
	httpPort, grpcPort, mysqlPort = 4000, 4001, 4002
)

type monitor struct {
	ID          int64     `gorm:"primaryKey;column:id"`
	Host        string    `gorm:"primaryKey;column:host"`
	Memory      uint64    `gorm:"column:memory"`
	Cpu         float64   `gorm:"column:cpu"`
	Temperature int64     `gorm:"column:temperature"`
	Running     bool      `gorm:"column:running"`
	Ts          time.Time `gorm:"column:ts"`
}

func (monitor) TableName() string {
	return "monitor"
}

type mysql_ struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string

	DB *gorm.DB
}

func (m *mysql_) Setup() error {
	if m.DB != nil {
		return nil
	}

	dsn := fmt.Sprintf("tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		m.Host, m.Port, m.Database)
	if m.User != "" && m.Password != "" {
		dsn = fmt.Sprintf("%s:%s@%s", m.User, m.Password, dsn)
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}

	m.DB = db
	return nil
}

func (p *mysql_) AllMonitors() ([]monitor, error) {
	var monitors []monitor
	err := p.DB.Find(&monitors).Error
	return monitors, err
}

func INIT() {
	repo := "greptime/greptimedb"
	tag := "v0.6.0"

	var err error
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalln("Could not connect to docker: " + err.Error())
	}

	log.Printf("Preparing container %s:%s\n", repo, tag)

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
		log.Fatalln("could not start resource: " + err.Error())
	}
	var expire uint = 30
	log.Println("Starting container...")

	err = resource.Expire(expire) // Tell docker to hard kill the container
	if err != nil {
		log.Printf("Expire container failed, %s\n", err.Error())
	}

	pool.MaxWait = 30 * time.Second

	if err := pool.Retry(func() error {
		time.Sleep(time.Second * 5)
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
	client, err := New(cfg)
	assert.Nil(t, err)
	return client
}

func newMysql(t *testing.T) *mysql_ {
	mysql := &mysql_{
		Host:     "127.0.0.1",
		Port:     "4002",
		User:     "",
		Password: "",
		Database: "public",
	}
	if err := mysql.Setup(); err != nil {
		log.Fatalln("failed to setup mysql" + err.Error())
	}
	return mysql
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

func TestInsertMonitor(t *testing.T) {
	tableName := "test_insert_monitor"
	monitors := []monitor{
		{
			ID:          1,
			Host:        "127.0.0.1",
			Memory:      1,
			Cpu:         1.0,
			Temperature: -1,
			Ts:          time.Now(),
			Running:     true,
		},
		{
			ID:          2,
			Host:        "127.0.0.2",
			Memory:      2,
			Cpu:         2.0,
			Temperature: -2,
			Ts:          time.Now(),
			Running:     true,
		},
	}

	table, err := tbl.New(tableName)
	assert.Nil(t, err)

	table.AddTagColumn("id", types.INT64)
	table.AddTagColumn("host", types.STRING)
	table.AddFieldColumn("memory", types.UINT64)
	table.AddFieldColumn("cpu", types.FLOAT64)
	table.AddFieldColumn("temperature", types.INT64)
	table.AddFieldColumn("running", types.BOOLEAN)
	table.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND)

	for _, monitor := range monitors {
		err := table.AddRow(monitor.ID, monitor.Host,
			monitor.Memory, monitor.Cpu, monitor.Temperature, monitor.Running,
			monitor.Ts)
		assert.Nil(t, err)
	}

	client := newClient(t)
	resp, err := client.Write(context.Background(), table)
	fmt.Printf("--------- err: %#v\n", err)
	assert.Nil(t, err)
	fmt.Printf("--------- resp: %#v\n", resp)
}
