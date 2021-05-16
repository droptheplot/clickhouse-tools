# clickhouse-tools

[![Go Report Card](https://goreportcard.com/badge/github.com/droptheplot/clickhouse-tools)](https://goreportcard.com/report/github.com/droptheplot/clickhouse-tools)
[![Test](https://github.com/droptheplot/clickhouse-tools/actions/workflows/test.yml/badge.svg)](https://github.com/droptheplot/clickhouse-tools/actions/workflows/test.yml)
[![Release](https://github.com/droptheplot/clickhouse-tools/actions/workflows/release.yml/badge.svg)](https://github.com/droptheplot/clickhouse-tools/actions/workflows/release.yml)

Different tools for ClickHouse.

## Usage

### Dump

```shell
clickhouse-tools dump \
  --orig=database1 \
  --dest=database2 \
  --url=https://path.to.clickhouse:8443 \
  --user=user1 \
  --password=password1 \
  --cert=path.to.certificate.crt \
  --on-cluster \
  --drop-table \
  > schema.sql
```

## Download

Executable is available in the [latest release](https://github.com/droptheplot/clickhouse-tools/releases/latest) assets.
