# Insert data into GreptimeDB

## Insert

```go
go run main.go
```

Output:

```log
2024/02/18 11:06:20 affected rows: 2
2024/02/18 11:06:20 affected rows: 2
```

## Query

```shell
mysql -h 127.0.0.1 -P 4002 public
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

mysql> select * from monitors_with_schema;
+------+-------+-------------+----------------------------+
| id   | host  | temperature | timestamp                  |
+------+-------+-------------+----------------------------+
|    1 | hello |         1.1 | 2024-02-18 03:12:05.033351 |
|    1 | hello |         1.1 | 2024-02-18 03:12:05.158068 |
|    2 | hello |         2.2 | 2024-02-18 03:12:05.033352 |
|    2 | hello |         2.2 | 2024-02-18 03:12:05.158076 |
+------+-------+-------------+----------------------------+
4 rows in set (0.01 sec)
```
