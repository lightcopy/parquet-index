# parquet-index
Spark SQL index for Parquet tables

[![Build Status](https://travis-ci.org/lightcopy/parquet-index.svg?branch=master)](https://travis-ci.org/lightcopy/parquet-index)
[![Coverage Status](https://coveralls.io/repos/github/lightcopy/parquet-index/badge.svg?branch=master)](https://coveralls.io/github/lightcopy/parquet-index?branch=master)
[![Join the chat at https://gitter.im/lightcopy/parquet-index](https://badges.gitter.im/lightcopy/parquet-index.svg)](https://gitter.im/lightcopy/parquet-index?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Overview
Package allows to create index for Parquet tables to reduce query latency when used for
_almost interactive_ analysis or point queries in Spark SQL. It is designed for use case when table
does not change frequently, but is used for queries often, e.g. using Thrift JDBC/ODBC server. When
indexed, schema and list of files (including partitioning) will be automatically resolved from index
metastore instead of inferring schema every time datasource is created.

> Project is **experimental and is in active development**. Any feedback, issues or PRs are welcome.

> Documentation reflects changes in `master` branch, for a documentation for specific version,
> please select version tag or branch.

### Metastore
Metastore keeps information about all indexed tables and can be created on local file system or HDFS
(see available options below) with support for in-memory cache of index (after first scan). Each
created index includes different statistics (min/max/null) and, optionally, column filters
statistics (e.g. bloom filters) on indexed columns.

### Supported predicates
Index is automatically enabled for scan when provided predicate contains one or several filters with
indexed columns; if no filters on indexed columns are provided, then normal scan is used, but with
benefits of already resolved partitions and schema. Applying min/max statistics and column filter
statistics (if available) happens after partition pruning. Statistics are kept per Parquet block
metadata. Note that performance also depends on values distribution and predicate selectivity.
Spark Parquet reader is used to read data.

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
- Indexed columns cannot be the same as partitioning columns
- Append mode is not yet supported for Parquet table when creating index
- Certain Spark versions are supported (see table below)

## Requirements
| Spark version | `parquet-index` latest version |
|---------------|--------------------------------|
| 1.6.x | Not supported |
| 2.0.0 | [0.2.0](http://spark-packages.org/package/lightcopy/parquet-index) |
| 2.0.1 | [0.2.0](http://spark-packages.org/package/lightcopy/parquet-index) |
| 2.0.2 | [0.2.0](http://spark-packages.org/package/lightcopy/parquet-index) |
| 2.1.x | Not supported |

## Linking
The `parquet-index` package can be added to Spark by using the `--packages` command line option.
For example, run this to include it when starting `spark-shell` (Scala 2.11.x):
```shell
 $SPARK_HOME/bin/spark-shell --packages lightcopy:parquet-index:0.2.0-s_2.11
```
Or for `pyspark` to use Python API (see section below):
```shell
$SPARK_HOME/bin/pyspark --packages lightcopy:parquet-index:0.2.1-SNAPSHOT-s_2.11
```
Change to `lightcopy:parquet-index:0.2.0-s_2.10` for Scala 2.10.x

## Options
Currently supported options, use `--conf key=value` on a command line to provide options similar to
other Spark configuration or add them to `spark-defaults.conf` file.

| Name | Description | Default |
|------|-------------|---------|
| `spark.sql.index.metastore` | Index metastore location, created if does not exist (`file:/folder`, `hdfs://host:port/folder`) | `./index_metastore`
| `spark.sql.index.parquet.filter.enabled` | When set to `true`, write filter statistics for indexed columns when creating table index, otherwise only min/max statistics are used. Filter statistics are used during filtering stage, if applicable (`true`, `false`) | `true`
| `spark.sql.index.parquet.filter.type` | When filter statistics enabled, select type of statistics to use when creating index (`bloom`) | `bloom`
| `spark.sql.index.parquet.filter.eagerLoading` | When set to `true`, read and load all filter statistics in memory the first time catalog is resolved, otherwise load them lazily as needed when evaluating predicate (`true`, `false`) | `false`
| `spark.sql.index.createIfNotExists` | When set to true, create index if one does not exist in metastore for the table, and will use all available columns for indexing (`true`, `false`) | `false`

## Example

### Scala API
Most of the API is defined in [DataFrameIndexManager](./src/main/scala/org/apache/spark/sql/DataFrameIndexManager.scala).
Usage is similar to Spark's `DataFrameReader`, but for `spark.index`. See example below on different
commands (runnable in `spark-shell`).

```scala
// Start spark-shell and create dummy table "codes.parquet", use repartition
// to create more or less generic situation with value distribution
spark.range(0, 1000000).
  select($"id", $"id".cast("string").as("code"), lit("xyz").as("name")).
  repartition(400).
  write.partitionBy("name").parquet("temp/codes.parquet")

import com.github.lightcopy.implicits._
// Create index for table, this will create index files in index_metastore,
// you can configure different options - see table above

// All Spark SQL modes are available ('append', 'overwrite', 'ignore', 'error')
// You can also use `.indexByAll` instead to choose all columns in schema that
// can be indexed
spark.index.create.
  mode("overwrite").indexBy($"id", $"code").parquet("temp/codes.parquet")

// Check if index for table exists, should return "true"
spark.index.exists.parquet("temp/codes.parquet")

// Query table using index, should return 1 record, and will scan only small
// number of files (1 file usually if filter statistics are enabled). This
// example uses filters on both columns, though any filters can be used,
// e.g. only on id or code
// Metastore will cache index catalog to reduce time for subsequent calls
val df = spark.index.parquet("temp/codes.parquet").
  filter($"id" === 123 && $"code" === "123")
df.collect

// Delete index in metastore, also invalidates cache
// no-op if there is such index does not exist
// (does NOT delete original table)
spark.index.delete.parquet("temp/codes.parquet")

// You can compare performance with this
val df = spark.read.parquet("temp/codes.parquet").
  filter($"id" === 123 && $"code" === "123")
df.collect
```

### Java API
To use indexing in Java create `QueryContext` based on `SparkSession` and invoke method `index()` to
get index functionality. Example below illustrates how to use indexing in standalone application.
```java
import com.github.lightcopy.QueryContext;

// Optionally use `config(key, value)` to specify additional index configuration
SparkSession spark = SparkSession.
  builder().
  master("local[*]").
  appName("Java example").
  getOrCreate();

// Create query context, entry point to working with parquet-index
QueryContext context = new QueryContext(spark);

// Create index by inferring columns from Parquet table
context.index().create().indexByAll().parquet("table.parquet");

// Create index by specifying index columns, you can also provide `Column` instances, e.g.
// `new Column[] { new Column("col1"), new Column("col2") }`.
// Mode can be provided as `org.apache.spark.sql.SaveMode` or String value
context.index().create().
  mode("overwrite").
  indexBy(new String[] { "col1", "col2" }).
  parquet("table.parquet");

// Check if index exists for the table
boolean exists = context.index().exists().parquet("table.parquet");

// Run query for indexed table
Dataset<Row> df = context.index().parquet("table.parquet").filter("col2 = 'c'");

// Delete index from metastore
context.index().delete().parquet("table.parquet");
```

### Python API
Following example shows usage of Python API (runnable in `pyspark`)
```python
from lightcopy.index import QueryContext

# Create QueryContext from SparkSession
context = QueryContext(spark)

# Create index in metastore for Parquet table 'table.parquet' using 'col1' and 'col2' columns
context.index.create.indexBy('col1', 'col2').parquet('table.parquet')

# Create index in metastore for Parquet table 'table.parquet' using all inferred columns and
# overwrite any existing index for this table
context.index.create.mode('overwrite').indexByAll().parquet('table.parquet')

# Check if index exists, returns 'True' if exists, otherwise 'False'
context.index.exists.parquet('table.parquet')

# Query index for table, returns DataFrame
df = context.index.parquet('table.parquet').filter('col1 = 123')

# Delete index from metastore, if index does not exist - no-op
context.index.delete.parquet('table.parquet')
```

## Building From Source
This library is built using `sbt`, to build a JAR file simply run `sbt package` from project root.

## Testing
Run `sbt test` from project root. See `.travis.yml` for CI build matrix.
