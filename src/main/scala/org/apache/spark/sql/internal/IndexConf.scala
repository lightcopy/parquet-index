/*
 * Copyright 2016 Lightcopy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.internal

import org.apache.spark.internal.config._
import org.apache.spark.sql.SparkSession

object IndexConf {
  import SQLConf.SQLConfigBuilder

  val METASTORE_LOCATION = SQLConfigBuilder("spark.sql.index.metastore").
    doc("Metastore location or root directory to store index information, will be created " +
      "if path does not exist").
    stringConf.
    createWithDefault("")

  val CREATE_IF_NOT_EXISTS = SQLConfigBuilder("spark.sql.index.createIfNotExists").
    doc("When set to true, creates index if one does not exist in metastore for the table").
    booleanConf.
    createWithDefault(false)

  val NUM_PARTITIONS = SQLConfigBuilder("spark.sql.index.partitions").
    doc("When creating index uses this number of partitions. If value is non-positive or not " +
      "provided then uses `sc.defaultParallelism * 3` or `spark.sql.shuffle.partitions` " +
      "configuration value, whichever is smaller").
    intConf.
    createWithDefault(0)

  val PARQUET_FILTER_STATISTICS_ENABLED =
    SQLConfigBuilder("spark.sql.index.parquet.filter.enabled").
    doc("When set to true, writes filter statistics for indexed columns when creating table " +
      "index, otherwise only min/max statistics are used. Filter statistics are always used " +
      "during filtering stage, if applicable").
    booleanConf.
    createWithDefault(true)

  val PARQUET_FILTER_STATISTICS_TYPE = SQLConfigBuilder("spark.sql.index.parquet.filter.type").
    doc("When filter statistics enabled, selects type of statistics to use when creating index. " +
      "Available options are `bloom`, `dict`").
    stringConf.
    createWithDefault("bloom")

  val PARQUET_FILTER_STATISTICS_EAGER_LOADING =
    SQLConfigBuilder("spark.sql.index.parquet.filter.eagerLoading").
    doc("When set to true, read and load all filter statistics in memory the first time catalog " +
      "is resolved, otherwise load them lazily as needed when evaluating predicate. " +
      "Eager loading removes IO of reading filter data from disk, but requires extra memory").
    booleanConf.
    createWithDefault(false)

  val PARQUET_TREE_STATISTICS_ENABLED = SQLConfigBuilder("spark.sql.index.parquet.tree.enabled").
    doc("When set to true, uses tree statistics when creating index instead of column statistics " +
      "to store min/max/nulls metadata").
    booleanConf.
    createWithDefault(false)

  /** Create new configuration from session SQLConf */
  def newConf(sparkSession: SparkSession): IndexConf = {
    new IndexConf(sparkSession.sessionState.conf)
  }
}

class IndexConf private[sql](val sqlConf: SQLConf) {
  import IndexConf._

  /** ************************ Index Params/Hints ******************* */

  def metastoreLocation: String = getConf(METASTORE_LOCATION)

  def createIfNotExists: Boolean = getConf(CREATE_IF_NOT_EXISTS)

  def parquetFilterEnabled: Boolean = getConf(PARQUET_FILTER_STATISTICS_ENABLED)

  def parquetFilterType: String = getConf(PARQUET_FILTER_STATISTICS_TYPE)

  def parquetFilterEagerLoading: Boolean = getConf(PARQUET_FILTER_STATISTICS_EAGER_LOADING)

  def parquetTreeEnabled: Boolean = getConf(PARQUET_TREE_STATISTICS_ENABLED)

  def numPartitions: Int = getConf(NUM_PARTITIONS)

  /** ********************** IndexConf functionality methods ************ */

  /** Set configuration for underlying SQLConf */
  def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    sqlConf.setConf(entry, value)
  }

  /** Set direct configuration key and value in SQLConf */
  def setConfString(key: String, value: String): Unit = {
    sqlConf.setConfString(key, value)
  }

  /** Get configuration from underlying SQLConf */
  def getConf[T](entry: ConfigEntry[T]): T = {
    sqlConf.getConf(entry)
  }

  /** Unset configuration from SQLConf */
  def unsetConf(entry: ConfigEntry[_]): Unit = {
    sqlConf.unsetConf(entry)
  }
}
