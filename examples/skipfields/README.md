# Insert data into GreptimeDB

When fields are marked with `greptime:"-"`, writing to these fields will be skipped.

```go
type Monitor struct {
  ID          int64     `greptime:"tag;column:id;type:int64"`
  Host        string    `greptime:"tag;column:host;type:string"`
  Memory      uint64    `greptime:"-"`
  Cpu         float64   `greptime:"field;column:cpu;type:float64"`
  Temperature int64     `greptime:"-"`
  Running     bool      `greptime:"field;column:running;type:boolean"`
  Ts          time.Time `greptime:"timestamp;column:ts;type:timestamp;precision:millisecond"`
}
```

## Insert

```go
go run main.go
```

Output:

```log
2024/12/10 09:30:40 affected rows: 1
2024/12/10 09:30:40 affected rows: 1
```

## Query

Your can using [MySQL Client](https://docs.greptime.com/user-guide/protocols/mysql) to query the data from GreptimeDB.

```shell
$ mysql -h 127.0.0.1 -P 4002

mysql> select *from monitors_with_skip_fields;
+------+-----------+------+---------+----------------------------+
| id   | host      | cpu  | running | ts                         |
+------+-----------+------+---------+----------------------------+
|    0 | 127.0.0.1 |  1.3 |       0 | 2024-12-10 09:30:40.709000 |
|    1 | 127.0.0.2 |  3.2 |       1 | 2024-12-10 09:30:40.709000 |
+------+-----------+------+---------+----------------------------+
2 rows in set (0.03 sec)
```
