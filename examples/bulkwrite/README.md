# Bulk write data into GreptimeDB

## Create table

```sql
CREATE TABLE bulk_insert_table (
  `id` Int64,
  `host` String,
  `temperature` Float64,
  `ts` TIMESTAMP(6),
  TIME INDEX (`ts`),
  PRIMARY KEY (`id`)
);
```

## Insert

```go
go run main.go
```

Output:

```log
2025/08/01 12:56:44 Writing 10 rows to table
Write successful! Affected rows: 10
```

## Query

Your can using [MySQL Client](https://docs.greptime.com/user-guide/protocols/mysql) to query the data from GreptimeDB.

```shell
$ mysql -h 127.0.0.1 -P 4002

mysql> select * from bulk_insert_table;
+------+--------+-------------+----------------------------+
| id   | host   | temperature | ts                         |
+------+--------+-------------+----------------------------+
|    0 | host-0 |        20.5 | 2025-08-01 04:56:44.464908 |
|    1 | host-1 |        21.5 | 2025-08-01 04:56:45.464908 |
|    2 | host-2 |        22.5 | 2025-08-01 04:56:46.464908 |
|    3 | host-3 |        23.5 | 2025-08-01 04:56:47.464908 |
|    4 | host-4 |        24.5 | 2025-08-01 04:56:48.464908 |
|    5 | host-5 |        25.5 | 2025-08-01 04:56:49.464908 |
|    6 | host-6 |        26.5 | 2025-08-01 04:56:50.464908 |
|    7 | host-7 |        27.5 | 2025-08-01 04:56:51.464908 |
|    8 | host-8 |        28.5 | 2025-08-01 04:56:52.464908 |
|    9 | host-9 |        29.5 | 2025-08-01 04:56:53.464908 |
+------+--------+-------------+----------------------------+
10 rows in set (0.01 sec)
```
