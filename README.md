# parquet-index
In-memory index for Parquet tables

[![Build Status](https://travis-ci.org/lightcopy/parquet-index.svg?branch=master)](https://travis-ci.org/lightcopy/parquet-index)
[![Coverage Status](https://coveralls.io/repos/github/lightcopy/parquet-index/badge.svg?branch=master)](https://coveralls.io/github/lightcopy/parquet-index?branch=master)
[![Join the chat at https://gitter.im/lightcopy/parquet-index](https://badges.gitter.im/lightcopy/parquet-index.svg)](https://gitter.im/lightcopy/parquet-index?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Overview
Package provides means to create index for Parquet tables to reduce query latency when used for
_almost interactive_ analysis or point queries. It is designed for use case when table does not
change frequently, but is used for queries often, e.g. using Thrift JDBC/ODBC server. When indexed,
schema and list of files (including partitioning) will be automatically resolved from index
metastore instead of inferring schema every time datasource is created.

### Metastore
Metastore keeps information about all indexed tables and can be created on local file system or HDFS
(see available options below). Each created index includes different statistics (min/max/null) and,
optionally, bloom filters on indexed columns.

### Filters
Index is enabled for scan when provided predicate contains one or several filters with indexed
columns (if none, then normal scan is used, but with benefits of already resolved partitions and
schema). Filter resolution happens after partition pruning (if available) and currently applies on
column metadata level only (per file). Note that performance also depends on values distribution and
predicate selectivity. Spark Parquet reader is used to read data.

Most of the Spark SQL predicates are supported to use statistics and/or column filter
(`EqualTo`, `In`, `GreaterThan`, `LessThan`, and others). Note that predicates work best for
equality or `isin` conditions and logical operators (`And`, `Or`, `Not`),
e.g. `$"a" === 1 && $"b" === "abc"` or `$"a".isin("a", "b", "c")`.

### Supported Spark SQL types
Currently only these types are supported for indexed columns:
- `IntegerType`
- `LongType`
- `StringType`

### Limitations
- Indexed columns must be top level primitive columns with types above
- Indexed columns cannot be the same as partitioning columns (which kind of makes sense)
- Append mode is not supported for Parquet table when creating index
- Bucketing is not supported, meaning that package does not take advantage of bucketing, so
bucketed table would be processed and indexed like standard partitioned table
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

| Name | Since | Example | Description | Default |
|------|:-----:|:-------:|-------------|---------|
| `spark.sql.index.metastore` | `0.1.0` | _file:/folder, hdfs://host:port/folder_ | Index metastore location, created if does not exist | _working directory_
| `spark.sql.index.parquet.bloom.enabled` | `0.1.0` | _true, false_ | When set to true, writes bloom filters for indexed columns when creating table index | _false_
| `spark.sql.index.createIfNotExists` | `0.2.0` | _true, false_ | When set to true, creates index if one does not exist in metastore for the table (will use all available columns for indexing) | _false_

## Example

### Scala API
Most of the API is defined in [DataFrameIndexManager](./src/main/scala/org/apache/spark/sql/DataFrameIndexManager.scala).
Usage is similar to Spark's `DataFrameReader`, but for `spark.index`.

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
// You can also use `.indexByAll` to index by all inferred columns
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
