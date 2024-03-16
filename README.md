[![Build Status](https://github.com/greptimeteam/greptimedb-ingester-go/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-ingester-go/blob/main/.github/workflows/ci.yml)
[![codecov](https://codecov.io/gh/GreptimeTeam/greptimedb-ingester-go/branch/main/graph/badge.svg?token=76KIKITADQ)](https://codecov.io/gh/GreptimeTeam/greptimedb-ingester-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/GreptimeTeam/greptimedb-ingester-go.svg)](https://pkg.go.dev/github.com/GreptimeTeam/greptimedb-ingester-go)

# GreptimeDB Go Ingester

Provide API to insert data into GreptimeDB.

## How To Use

### Installation

```sh
go get -u github.com/GreptimeTeam/greptimedb-ingester-go
```

### Import

```go
import greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
```

### Config

Initiate a Config for Client

```go
cfg := greptime.NewConfig("<host>").
    WithPort(4001).
    WithAuth("<username>", "<password>").
    WithDatabase("<database>")
```

#### Options

##### Secure

```go
cfg.WithInsecure(false) // default insecure=true
```

##### keepalive

```go
cfg.WithKeepalive(time.Second*30, time.Second*5) // keepalive isn't enabled by default
```

### Client

```go
cli, err := greptime.NewClient(cfg)
```

### Insert & StreamInsert

- you can Insert data into GreptimeDB via different style:

  - [Table style](#table-style)
  - [ORM style](#orm-style)

- streaming insert is to Send data into GreptimeDB without waiting for response.

#### Table style

you can define schema via Table and Column, and then AddRow to include the real data you want to write.

##### define table schema, and add rows

```go
import(
    "github.com/GreptimeTeam/greptimedb-ingester-go/table"
    "github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

tbl, err := table.New("<table_name>")

tbl.AddTagColumn("id", types.INT64)
tbl.AddFieldColumn("host", types.STRING)
tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND)

err := tbl.AddRow(1, "127.0.0.1", time.Now())
err := tbl.AddRow(2, "127.0.0.2", time.Now())
...
```

##### Write into GreptimeDB

```go
resp, err := cli.Write(context.Background(), tbl)
```

##### Delete in GreptimeDB

```go
dtbl, err := table.New("<table_name>")
dtbl.AddTagColumn("id", types.INT64)
dtbl.AddFieldColumn("host", types.STRING)
dtbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND)

// timestamp is the time you want to delete row
err := dtbl.AddRow(1, "127.0.0.1",timestamp)

affected, err := cli.Delete(context.Background(),dtbl)
```

##### Stream Write into GreptimeDB

```go
err := cli.StreamWrite(context.Background(), tbl)
...
affected, err := cli.CloseStream(ctx)
```
##### Stream Delete in GreptimeDB
    
```go
err := cli.StreamDelete(context.Background(), tbl)
...
affected, err := cli.CloseStream(ctx)
```

#### ORM style

If you prefer ORM style, and define column-field relationship via struct field tag, you can try the following way.

##### Tag

- `greptime` is the struct tag key
- `tag`, `field`, `timestamp` is for [SemanticType][data-model], and the value is ignored
- `column` is to define the column name
- `type` is to define the data type. if type is timestamp, `precision` is supported
- the metadata separator is `;` and the key value separator is `:`

type supported is the same as described [Datatypes supported](#datatypes-supported), and case insensitive

##### define struct with tags

```go
type Monitor struct {
    ID          int64     `greptime:"tag;column:id;type:int64"`
    Host        string    `greptime:"field;column:host;type:string"`
    Ts          time.Time `greptime:"timestamp;column:ts;type:timestamp;precision:millisecond"`
}

// TableName is to define the table name.
func (Monitor) TableName() string {
    return "<table_name>"
}
```

##### instance your struct

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

##### WriteObject into GreptimeDB

```go
resp, err := cli.WriteObject(context.Background(), monitors)
```

##### DeleteObject in GreptimeDB

```go
deleteMonitors := monitors[:1]

affected, err := cli.DeleteObject(context.Background(), deleteMonitors)
```

##### Stream WriteObject into GreptimeDB

```go
err := cli.StreamWriteObject(context.Background(), monitors)
...
affected, err := cli.CloseStream(ctx)
```

##### Stream DeleteObject in GreptimeDB

```go
deleteMonitors := monitors[:1]

err := cli.StreamDeleteObject(context.Background(), deleteMonitors)
...
affected, err := cli.CloseStream(ctx)
```

## Datatypes supported

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
| DATE                             | *Int* or time.Time | the day elapsed since 1970-1-1         |
| DATETIME                         | *Int* or time.Time | the millisecond elapsed since 1970-1-1 |
| TIMESTAMP_SECOND                 | *Int* or time.Time |                                        |
| TIMESTAMP_MILLISECOND, TIMESTAMP | *Int* or time.Time |                                        |
| TIMESTAMP_MICROSECOND            | *Int* or time.Time |                                        |
| TIMESTAMP_NANOSECOND             | *Int* or time.Time |                                        |

NOTE: *Int* is for all of Integer and Unsigned Integer in Go

## Query

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
