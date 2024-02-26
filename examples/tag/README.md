# Insert data into GreptimeDB

## Insert

```go
go run main.go
```

Output:

```log
2024/02/07 11:26:26 affected rows: 2
```

## Query

```shell
2024/02/18 11:14:54 affected rows: 2
2024/02/18 11:14:54 affected rows: 2
```

```shell
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 8
Server version: 5.1.10-alpha-msql-proxy Greptime

Copyright (c) 2000, 2023, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> select * from monitors_with_tag;
+------+-----------+--------+------+-------------+---------+----------------------------+
| id   | host      | memory | cpu  | temperature | running | ts                         |
+------+-----------+--------+------+-------------+---------+----------------------------+
|    1 | 127.0.0.1 |      1 |    1 |          -1 |       1 | 2024-02-18 03:14:54.116000 |
|    1 | 127.0.0.1 |      1 |    1 |          -1 |       1 | 2024-02-18 03:14:54.242000 |
|    2 | 127.0.0.2 |      2 |    2 |          -2 |       1 | 2024-02-18 03:14:54.116000 |
|    2 | 127.0.0.2 |      2 |    2 |          -2 |       1 | 2024-02-18 03:14:54.242000 |
+------+-----------+--------+------+-------------+---------+----------------------------+
4 rows in set (0.01 sec)
```
