# parquet-index

[![Join the chat at https://gitter.im/parquet-index/Lobby](https://badges.gitter.im/parquet-index/Lobby.svg)](https://gitter.im/parquet-index/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
In-memory index for Parquet tables

[![Build Status](https://travis-ci.org/lightcopy/parquet-index.svg?branch=master)](https://travis-ci.org/lightcopy/parquet-index)
[![Coverage Status](https://coveralls.io/repos/github/lightcopy/parquet-index/badge.svg?branch=master)](https://coveralls.io/github/lightcopy/parquet-index?branch=master)

## Overview
Package provides means to create index for Parquet tables to reduce query latency when used for
_almost interactive_ analysis or point queries. It is designed for use case when table does not
change frequently, but is used for queries often, e.g. using Thrift JDBC/ODBC server. When indexed,
schema and list of files (including partitioning) will be automatically resolved from index
metastore instead of inferring schema every time datasource is created.

Metastore keeps information about all indexed tables and can be created on local file system or HDFS
(see available options below). Each created index includes different statistics (min/max/null) and,
optionally, bloom filters on indexed columns.

Index is enabled for scan when provided predicate contains one or several filters with indexed
columns. Filter resolution happens after partition pruning (if available) and currently applies on
column metadata level only (per file). Note that performance also depends on values distribution and
predicate selectivity. Spark Parquet reader is used to read data.

### Supported Spark SQL types
Currently only these types are supported for indexed columns:
- `IntegerType`
- `LongType`
- `StringType`

### Limitations
- At least one indexed column should be provided, package does not store just schema
- Indexed columns must be top level primitive columns with types above
- Indexed columns cannot be the same as partitioning columns (which kind of makes sense)
- Append mode is not supported for Parquet table when creating index
- Bucketing is not supported, meaning that package does not take advantage of bucketing, so
bucketed table would be processed and indexed like standard partitioned table
- Only equality and logical (`And`, `Or`) predicates are supported, e.g. `col("a") === 1 && col("b") === "str"`
- Certain Spark versions are supported (see table below)

Project is **experimental and is in active development at the moment**. We are working to remove
limitations and add support for different versions. Any feedback, issues or PRs are welcome.

## Requirements
| Spark version | `parquet-index` latest version |
|---------------|--------------------------------|
| 1.6.x | Not supported |
| 2.0.0 | Not supported |
| 2.0.1 | [0.1.0](http://spark-packages.org/package/lightcopy/parquet-index) |
| 2.0.2 | [0.1.0](http://spark-packages.org/package/lightcopy/parquet-index) |
| 2.1.x | Not supported |

## Linking
The `parquet-index` package can be added to Spark by using the `--packages` command line option.
For example, run this to include it when starting the spark shell (Scala 2.10.x):
```shell
 $SPARK_HOME/bin/spark-shell --packages lightcopy:parquet-index:0.1.0-s_2.10
```
Change to `lightcopy:parquet-index:0.1.0-s_2.11` for Scala 2.11.x

## Options
Currently supported options, use `--conf key=value` on a command line to provide options similar to
other Spark configuration or add them to `spark-defaults.conf` file.

| Name | Since | Example | Description |
|------|:-----:|:-------:|-------------|
| `spark.sql.index.metastore` | `0.1.0` | _file:/folder, hdfs://host:port/folder_ | Index metastore location, by default uses current working directory; created if does not exist
| `spark.sql.index.parquet.bloom.enabled` | `0.1.0` | _true, false_ | When set to true, writes bloom filters for indexed columns when creating table index; by default is `false`

## Example

### Scala API
```scala
// Create dummy table "codes.parquet", use repartition to create more or less generic
// situation with value distribution
spark.range(0, 10000).
  select($"id", $"id".cast("string").as("code"), lit("xyz").as("name")).
  repartition(200).
  write.partitionBy("name").parquet("temp/codes.parquet")

// Create index for table, this will create index files in index_metastore
// see for supported types for indexed columns
import com.github.lightcopy.implicits._

// All Spark SQL modes are available (append, overwrite, ignore, error)
spark.index.create.
  mode("overwrite").indexBy($"id", $"code").parquet("temp/codes.parquet")

// Check if index for table exists, should return "true"
spark.index.exists.parquet("temp/codes.parquet")

// Query table using index, should return 1 record, and will scan
// only small number of files. This example uses filters on both columns,
// though any filters can be used, e.g. only on id or code
val df = spark.index.parquet("temp/codes.parquet").
  filter($"id" === 123 && $"code" === "123")
df.collect

// Delete index, no-op if there is no index for table
spark.index.delete.parquet("temp/codes.parquet")

// You can compare performance with this
val df = spark.read.parquet("temp/codes.parquet").
  filter($"id" === 123 && $"code" === "123")
df.collect
```

## Building From Source
This library is built using `sbt`, to build a JAR file simply run `sbt package` from project root.

## Testing
Run `sbt test` from project root. See `.travis.yml` for CI build matrix.
