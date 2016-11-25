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

package com.github.lightcopy

import scala.collection.mutable.HashMap

import org.apache.spark.sql.{Column, DataFrame, SaveMode, SQLContext}


/**
 * Interface to prepare indexed `DataFrame`, and access index functionality, e.g. create, drop,
 * alter existing index.
 * @param sqlContext SQLContext for the session
 */
private[lightcopy] class DataFrameIndexBuilder(@transient val sqlContext: SQLContext) {
  // source to use when searching index
  private var source: String = null
  // path to the datasource
  private var path: Option[String] = None
  // save mode for index
  private var mode: SaveMode = SaveMode.ErrorIfExists
  // extra configuration per index
  private val extraOptions = new HashMap[String, String]()

  /**
   * Format to read provided index from metastore.
   * @param source string identifier for format, e.g. "parquet"
   */
  def format(source: String): DataFrameIndexBuilder = {
    this.source = source
    this
  }

  /**
   * Path to the datasource table, should be non-empty, without glob expansions or regular
   * expressions, e.g. unique path to identify datasource table. This is optional and applies for
   * file system datasources, e.g. Parquet.
   * @param path path to the datasource table
   */
  def path(path: String): DataFrameIndexBuilder = {
    this.path = Option(path)
    this
  }

  /**
   * Add specific option for this index builder. This applies only to current index.
   * @param key option key, e.g. "merge"
   * @param value option value, e.g. "true"
   */
  def option(key: String, value: String): DataFrameIndexBuilder = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * (Scala-only) Add set of index options for index builder.
   * @param options options map
   */
  def options(options: Map[String, String]): DataFrameIndexBuilder = {
    this.extraOptions ++= options
    this
  }

  /**
   * Short-cut option to index Parquet table. Note that path must point a root directory for all
   * Parquet files with mergeable schema, preferably one that is saved from Spark.
   */
  def parquet(path: String): DataFrameIndexBuilder = {
    format("parquet").path(path)
  }

  /**
   * Mode for creating index based on all information gathered by [[DataFrameIndexBuilder]], has
   * the same meaning as one provided when saving DataFrame. It is ignored for other operations.
   * @param mode save mode for index
   */
  def mode(mode: SaveMode): DataFrameIndexBuilder = {
    this.mode = mode
    this
  }

  /**
   * Mode for string value.
   * @param mode value for save mode
   */
  def mode(mode: String): DataFrameIndexBuilder = {
    this.mode = mode match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" => SaveMode.ErrorIfExists
      case other => throw new IllegalArgumentException(s"Unknown save mode: $other. " +
        "Accepted save modes are 'overwrite', 'append', 'ignore', 'error'.")
    }
    this
  }
  //////////////////////////////////////////////////////////////
  // == Catalog functionality ==
  //////////////////////////////////////////////////////////////

  /**
   * Create index for provided columns, at least one column required. Column must be resolved, e.g.
   * cannot be "*", or nested column, e.g. "map.*". See `mode()` method for different create
   * functionality.
   * @param cols columns to build index, subsequent query will be applied on those columns
   */
  def create(cols: Column*): Unit = {
    throw new UnsupportedOperationException()
  }

  /** Drop provided index. If index does not exist, no-op */
  def drop(): Unit = {
    throw new UnsupportedOperationException()
  }

  /**
   * Return DataFrame based on filtering condition. If condition includes indexed columns, those
   * will be applied to read metastore, unsupported filtering will be applied on resulting
   * DataFrame. If filtering columns do not contain any indexed columns, will use Spark datasource
   * API to return result. Note that if filtered result is of high cardinality, e.g. many blocks to
   * read, standard datasource scan is used to read table, instead of using index.
   * @param condition filtering expression similar to `DataFrame.filter(...)`
   */
  def query(condition: Column): DataFrame = {
    throw new UnsupportedOperationException()
  }
}
