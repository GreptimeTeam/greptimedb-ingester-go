/*
with schema predefined
*/
package main

import (
	"context"
	"log"
	"time"

	"github.com/GreptimeTeam/greptimedb-ingester-go/client"
	"github.com/GreptimeTeam/greptimedb-ingester-go/config"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

var (
	cli    *client.Client
	stream *client.StreamClient
)

func init() {
	cfg := config.New("127.0.0.1").WithDatabase("public")

	cli_, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	cli = cli_

	stream_, err := client.NewStreamClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	stream = stream_
}

func main() {
	tbl, err := table.New("monitors_with_schema")
	if err != nil {
		log.Fatal(err)
	}

	// add column at first. This is to define the schema of the table.
	if err := tbl.AddTagColumn("id", types.INT64); err != nil {
		log.Fatal(err)
	}
	if err := tbl.AddFieldColumn("host", types.STRING); err != nil {
		log.Fatal(err)
	}
	if err := tbl.AddFieldColumn("temperature", types.FLOAT); err != nil {
		log.Fatal(err)
	}
	if err := tbl.AddTimestampColumn("timestamp", types.TIMESTAMP_MICROSECOND); err != nil {
		log.Fatal(err)
	}

	if err := tbl.AddRow(1, "hello", 1.1, time.Now()); err != nil {
		log.Fatal(err)
	}
	if err := tbl.AddRow(2, "hello", 2.2, time.Now()); err != nil {
		log.Fatal(err)
	}

	{ // client write data into GreptimeDB
		resp, err := cli.Write(context.Background(), tbl)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
	}

	{ // stream client send data into GreptimeDB
		err := stream.Send(context.Background(), tbl)
		if err != nil {
			log.Fatal(err)
		}
	}

}
