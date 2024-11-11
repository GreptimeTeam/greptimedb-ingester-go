# Insert data into GreptimeDB

## Insert

```go
go run main.go
```

Output:

```log
2024/11/11 14:59:56 affected rows: 1
```

## Query

Your can using [MySQL Client](https://docs.greptime.com/user-guide/protocols/mysql) to query the data from GreptimeDB.

```shell
$ mysql -h 127.0.0.1 -P 4002

mysql> select * from json_data;
+-----------------------------------------------------------------------------------------+----------------------------+
| my_json                                                                                 | timestamp                  |
+-----------------------------------------------------------------------------------------+----------------------------+
| {"Age":25,"Courses":["math","history","chemistry"],"IsStudent":false,"Name":"Jain Doe"} | 2024-11-11 06:59:56.340132 |
+-----------------------------------------------------------------------------------------+----------------------------+
1 row in set (0.04 sec)
```

You can view table fields using `show create table` command:

```mysql
mysql> show create table json_data;
+-----------+-------------------------------------------------------------------------------------------------------------------------------------------------+
| Table     | Create Table                                                                                                                                    |
+-----------+-------------------------------------------------------------------------------------------------------------------------------------------------+
| json_data | CREATE TABLE IF NOT EXISTS `json_data` (
  `my_json` JSON NULL,
  `timestamp` TIMESTAMP(6) NOT NULL,
  TIME INDEX (`timestamp`)
)

ENGINE=mito
 |
+-----------+-------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
