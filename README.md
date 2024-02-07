[![Build Status](https://github.com/greptimeteam/greptimedb-ingester-go/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-ingester-go/blob/main/.github/workflows/ci.yml)
[![codecov](https://codecov.io/gh/GreptimeTeam/greptimedb-ingester-go/branch/main/graph/badge.svg?token=76KIKITADQ)](https://codecov.io/gh/GreptimeTeam/greptimedb-ingester-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/GreptimeTeam/greptimedb-ingester-go.svg)](https://pkg.go.dev/github.com/GreptimeTeam/greptimedb-ingester-go)

# GreptimeDB Go Ingester

Provide API to insert data into GreptimeDB.

## Basic Example

- [schema](examples/schema/main.go)
- [tag](examples/tag/main.go)

## How To Use

### Installation

```sh
go get -u github.com/GreptimeTeam/greptimedb-ingester-go
```

### Example

#### Config

Initiate a Config for Client or StreamClient

```go
cfg := config.New("<host>").
    WithAuth("<username>", "<password>").
    WithDatabase(database)
```

##### Options

- keepalive

```go
cfg = cfg.WithKeepalive(30*time.Second, 5*time.Second)
```

#### Client or StreamClient

- Client

```go
cli, err := client.New(cfg)
```

- StreamClient

```go

stream, err := client.NewStreamClient(cfg)
```

#### Insert & StreamInsert

- you can Insert data into GreptimeDB via:
  - [define schema](#with-schema-predefined)
  - [define struct](#with-struct-tag)
- streaming insert is to Send data into GreptimeDB without waiting for response.

##### Datatypes supported

The **GreptimeDB** column is for the datatypes supported in library, and the **Go** column is the matched Go type.

| GreptimeDB                       | Go               | Description                            |
|----------------------------------|------------------|----------------------------------------|
| INT8                             | int8             |                                        |
| INT16                            | int16            |                                        |
| INT32                            | int32            |                                        |
| INT64, INT                       | int64            |                                        |
| UINT8                            | uint8            |                                        |
| UINT16                           | uint16           |                                        |
| UINT32                           | uint32           |                                        |
| UINT64, UINT                     | uint64           |                                        |
| FLOAT32                          | float32          |                                        |
| FLOAT64, FLOAT                   | float64          |                                        |
| BOOLEAN, BOOL                    | bool             |                                        |
| STRING                           | string           |                                        |
| BINARY, BYTES                    | []byte           |                                        |
| DATE                             | Int or time.Time | the day elapsed since 1970-1-1         |
| DATETIME                         | Int or time.Time | the millisecond elapsed since 1970-1-1 |
| TIMESTAMP_SECOND                 | Int or time.Time |                                        |
| TIMESTAMP_MILLISECOND, TIMESTAMP | Int or time.Time |                                        |
| TIMESTAMP_MICROSECOND            | Int or time.Time |                                        |
| TIMESTAMP_NANOSECOND             | Int or time.Time |                                        |

NOTE: Int is for all of Integer and Unsigned Integer in Go

##### With Schema predefined

###### define table schema, and add rows

```go
tbl, err := table.New("<table_name>")

tbl.AddTagColumn("id", types.INT64)
tbl.AddFieldColumn("host", types.STRING)
tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND)

err := tbl.AddRow(1, "127.0.0.1", time.Now())
err := tbl.AddRow(2, "127.0.0.2", time.Now())
...
```

###### Client Write into GreptimeDB

```go
resp, err := cli.Write(context.Background(), tbl)
```

###### StreamClient Send into GreptimeDB

```go
err := streamClient.Send(context.Background(), tbl)
```

##### With Struct Tag

###### Tag

- `greptime` is the tag key
- `tag`, `field`, `timestamp` is for [SemanticType][data-model]
- `column` is to define the column name
- `type` is to define the data type. if type is timestamp, `precision` is supported

type supported is the same as described [Datatypes supported in GreptimeDB](#greptimedb), and case insensitive.

###### define struct with tags

```go
type Monitor struct {
    ID          int64     `greptime:"tag;column:id;type:int64"`
    Host        string    `greptime:"field;column:host;type:string"`
    Ts          time.Time `greptime:"timestamp;column:ts;type:timestamp;precision:millisecond"`
}

// TableName is to define the table name.
// if TableName method is not found, the snake style and lower case of struct name
// will be used as table name. The table name is `monitor` if TableName method not found.
func (Monitor) TableName() string {
    return "<table_name>"
}
```

###### instance your struct

```go
monitors := []Monitor{
    {
        ID:          randomId(),
        Host:        "127.0.0.1",
        Running:     true,
    },
    {
        ID:          randomId(),
        Host:        "127.0.0.2",
        Running:     true,
    },
}
```

###### Client Create into GreptimeDB

```go
resp, err := cli.Create(context.Background(), monitors)
```

###### StreamClient Create into GreptimeDB

```go
err := streamClient.Create(context.Background(), monitors)
```

#### Query

You can use ORM library like [gorm][gorm] with MySQL or PostgreSQL driver to [connect][connect] GreptimeDB and retrieve data from it.

```go
type Monitor struct {
    ID          int64     `gorm:"primaryKey;column:id"`
    Host        string    `gorm:"column:host"`
    Ts          time.Time `gorm:"column:ts"`
}

// Get all monitors
var monitors []Monitor
result := db.Find(&monitors)
```

[gorm]: https://gorm.io/
[connect]: https://gorm.io/docs/connecting_to_the_database.html
[data-model]: https://docs.greptime.com/user-guide/concepts/data-model
