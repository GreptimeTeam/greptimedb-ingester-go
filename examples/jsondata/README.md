# Insert data into GreptimeDB

## Insert

```go
go run main.go
```

Output:

```log
2024/11/27 14:26:54 affected rows: 1
2024/11/27 14:26:54 affected rows: 1
2024/11/27 14:26:54 affected rows: 1
```

## Query

Your can using [MySQL Client](https://docs.greptime.com/user-guide/protocols/mysql) to query the data from GreptimeDB.

```shell
$ mysql -h 127.0.0.1 -P 4002

mysql> select *from json_data;
+------+------------------------------------------------------------------------------------+----------------------------+
| id   | my_json                                                                            | ts                         |
+------+------------------------------------------------------------------------------------+----------------------------+
|    1 | {"Age":25,"Courses":["math","history","chemistry"],"IsStudent":false,"Name":"doe"} | 2024-11-27 14:26:54.772697 |
|    2 | {"city":"New York","description":"Partly cloudy","temperature":22}                 | 2024-11-27 14:26:54.772698 |
|    3 | {"Age":23,"Courses":["archaeology","physics"],"IsStudent":true,"Name":"Cherry"}    | 2024-11-27 14:26:54.772698 |
+------+------------------------------------------------------------------------------------+----------------------------+
3 rows in set (0.04 sec)
```

You can view table fields using `show create table` command:

```mysql
mysql> show create table json_data;
+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Table     | Create Table                                                                                                                                                                |
+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| json_data | CREATE TABLE IF NOT EXISTS `json_data` (
  `id` BIGINT NULL,
  `my_json` JSON NULL,
  `ts` TIMESTAMP(6) NOT NULL,
  TIME INDEX (`ts`),
  PRIMARY KEY (`id`)
)
ENGINE=mito
|
+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.02 sec)
```
