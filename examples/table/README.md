# Insert data into GreptimeDB

## Insert

```go
go run main.go
```

Output:

```log
2024/03/23 23:04:19 affected rows: 3
2024/03/23 23:04:19 affected rows: 1
2024/03/23 23:04:19 affected rows: 1
2024/03/23 23:04:19 affected rows: 3
2024/03/23 23:04:19 affected rows: 1
2024/03/23 23:04:19 affected rows: 1
```

## Query

```shell
$ mysql -h 127.0.0.1 -P 4002 public

mysql> select * from monitors_with_schema;
+------+-------+-------------+----------------------------+
| id   | host  | temperature | timestamp                  |
+------+-------+-------------+----------------------------+
|    1 | hello |         1.2 | 2024-03-23 15:04:19.631482 |
|    1 | hello |         1.2 | 2024-03-23 15:04:19.740767 |
|    2 | hello |         2.2 | 2024-03-23 15:04:19.631482 |
|    2 | hello |         2.2 | 2024-03-23 15:04:19.740768 |
+------+-------+-------------+----------------------------+
4 rows in set (0.01 sec)
```
