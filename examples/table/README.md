# Insert data into GreptimeDB

## Insert

```go
go run main.go
```

Output:

```log
2025/03/03 04:21:20 affected rows: 3
2025/03/03 04:21:20 affected rows: 1
2025/03/03 04:21:20 affected rows: 1
2025/03/03 04:21:20 affected rows: 3
2025/03/03 04:21:20 affected rows: 1
2025/03/03 04:21:20 affected rows: 1
```

## Query

```shell
$ mysql -h 127.0.0.1 -P 4002 public

mysql> select * from monitors_with_schema;
+------+-------+-------------+----------------------------+
| id   | host  | temperature | timestamp                  |
+------+-------+-------------+----------------------------+
|    1 | hello |         1.2 | 2025-03-03 04:21:20.707171 |
|    1 | hello |         1.2 | 2025-03-03 04:21:20.862274 |
|    2 | hello |         2.2 | 2025-03-03 04:21:20.707171 |
|    2 | hello |         2.2 | 2025-03-03 04:21:20.862274 |
+------+-------+-------------+----------------------------+
4 rows in set (0.05 sec)
```
