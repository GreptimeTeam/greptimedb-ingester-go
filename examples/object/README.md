# Insert data into GreptimeDB

## Insert

```go
go run main.go
```

Output:

```log
2024/03/23 22:36:06 affected rows: 3
2024/03/23 22:36:06 affected rows: 3
2024/03/23 22:36:06 affected rows: 1
2024/03/23 22:36:06 affected rows: 3
2024/03/23 22:36:06 affected rows: 3
2024/03/23 22:36:06 affected rows: 1
```

## Query

Your can using [MySQL Client](https://docs.greptime.com/user-guide/clients/mysql) to query the data from GreptimeDB.

```shell
$ mysql -h 127.0.0.1 -P 4002 public

mysql> select * from monitors_with_tag;
+------+-----------+--------+------+-------------+---------+----------------------------+
| id   | host      | memory | cpu  | temperature | running | ts                         |
+------+-----------+--------+------+-------------+---------+----------------------------+
|    1 | 127.0.0.1 |      1 |  1.1 |          -1 |       1 | 2024-03-23 14:36:06.591000 |
|    1 | 127.0.0.1 |      1 |  1.1 |          -1 |       1 | 2024-03-23 14:36:06.732000 |
|    2 | 127.0.0.2 |      2 |    2 |          -2 |       1 | 2024-03-23 14:36:06.591000 |
|    2 | 127.0.0.2 |      2 |    2 |          -2 |       1 | 2024-03-23 14:36:06.732000 |
+------+-----------+--------+------+-------------+---------+----------------------------+
4 rows in set (0.12 sec)
```
