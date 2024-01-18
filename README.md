[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/GreptimeTeam/greptimedb-ingester-go/blob/main/LICENSE)
[![Build Status](https://github.com/greptimeteam/greptimedb-ingester-go/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-ingester-go/blob/main/.github/workflows/ci.yml)
[![codecov](https://codecov.io/gh/GreptimeTeam/greptimedb-ingester-go/branch/main/graph/badge.svg?token=76KIKITADQ)](https://codecov.io/gh/GreptimeTeam/greptimedb-ingester-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/GreptimeTeam/greptimedb-ingester-go.svg)](https://pkg.go.dev/github.com/GreptimeTeam/greptimedb-ingester-go)

# GreptimeDB Go Ingester

NOTE: the project is still in its early stages.

Provide API for using GreptimeDB client in Go.

## Installation

```sh
go get -u github.com/GreptimeTeam/greptimedb-ingester-go
```

## Documentation

visit [docs](./docs) to get complete examples. You can also visit [Documentation][document] more details.

## API reference

### Datatype Supported

- int8, int16, int32, int64, int
- uint8, uint16, uint32, uint64, uint
- float32, float64
- bool
- []byte
- string
- time.Time

### Customize metric Timestamp

you can customize timestamp index via calling methods of [Metric][metric_doc]

- `metric.SetTimePrecision(time.Microsecond)`
- `metric.SetTimestampAlias("timestamp")`

## License

This greptimedb-ingester-go uses the __Apache 2.0 license__ to strike a balance
between open contributions and allowing you to use the software however you want.

<!-- links -->
[document]: https://pkg.go.dev/github.com/GreptimeTeam/greptimedb-ingester-go
[metric_doc]: https://pkg.go.dev/github.com/GreptimeTeam/greptimedb-ingester-go#Metric
