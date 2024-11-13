# Insert data into GreptimeDB

## Insert

```go
go run main.go
```

Output:

```log
2024/11/13 19:39:37 affected rows: 3
2024/11/13 19:39:37 affected rows: 1
2024/11/13 19:39:37 affected rows: 1
2024/11/13 19:39:37 affected rows: 3
2024/11/13 19:39:37 affected rows: 1
2024/11/13 19:39:37 affected rows: 1
```

## Query

```shell
$ mysql -h 127.0.0.1 -P 4002 public

mysql> select * from monitors_with_schema;
+------+-------+-------------+----------------------------+
| id   | host  | temperature | timestamp                  |
+------+-------+-------------+----------------------------+
|    1 | hello |         1.1 | 2024-11-13 11:39:37.237320 |
|    1 | hello |         1.1 | 2024-11-13 11:39:37.417887 |
|    2 | hello |         2.2 | 2024-11-13 11:39:37.237320 |
|    2 | hello |         2.2 | 2024-11-13 11:39:37.417887 |
|    3 | hello |         3.3 | 2024-11-13 11:39:37.237320 |
|    3 | hello |         3.3 | 2024-11-13 11:39:37.417887 |
+------+-------+-------------+----------------------------+
6 rows in set (0.03 sec)
```
