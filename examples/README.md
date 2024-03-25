# Insert data into GreptimeDB

## Start local GreptimeDB instance via Docker

```shell
docker run --rm -p 4000-4003:4000-4003 \
--name greptime greptime/greptimedb standalone start \
--http-addr 0.0.0.0:4000 \
--rpc-addr 0.0.0.0:4001 \
--mysql-addr 0.0.0.0:4002 \
--postgres-addr 0.0.0.0:4003
```

## Examples

- [table](table/README.md)
- [object](object/README.md)

## Query

Your can using [MySQL Client](https://docs.greptime.com/user-guide/clients/mysql) to query the data from GreptimeDB.
