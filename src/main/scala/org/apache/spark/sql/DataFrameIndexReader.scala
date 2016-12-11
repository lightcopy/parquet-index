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

package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.IndexedDataSource
import org.apache.spark.sql.types.StructType

class DataFrameIndexReader(sparkSession: SparkSession) {
  def format(source: String): DataFrameIndexReader = {
    this.source = source
    this
  }

  def mode(mode: SaveMode): DataFrameIndexReader = {
    this.mode = mode
    this
  }

  def mode(mode: String): DataFrameIndexReader = {
    val typedMode = mode.toLowerCase match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "error" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
      case other => throw new UnsupportedOperationException(
        s"Unsupported mode $mode, must be one of ${SaveMode.Append}, ${SaveMode.Overwrite}, " +
        s"${SaveMode.ErrorIfExists}, ${SaveMode.Ignore}")
    }
    this.mode = typedMode
    this
  }

  def option(key: String, value: String): DataFrameIndexReader = {
    this.extraOptions += (key -> value)
    this
  }

  def option(key: String, value: Boolean): DataFrameIndexReader = option(key, value.toString)

  def option(key: String, value: Long): DataFrameIndexReader = option(key, value.toString)

  def option(key: String, value: Double): DataFrameIndexReader = option(key, value.toString)

  def options(options: scala.collection.Map[String, String]): DataFrameIndexReader = {
    this.extraOptions ++= options
    this
  }

  def load(path: String): DataFrame = {
    option("path", path)
    sparkSession.baseRelationToDataFrame(
      IndexedDataSource(
        sparkSession,
        className = source,
        mode = mode,
        options = extraOptions.toMap).resolveRelation())
  }

  /**
   * Create indexed DataFrame from Parquet table.
   * @param path filepath to the Parquet table (directory)
   */
  def parquet(path: String): DataFrame = {
    format("parquet").load(path)
  }

  /**
   * Create index for table with path and columns to index.
   * @param path filepath to the Parquet table (directory)
   * @param cols set of columns to index by
   */
  def create(path: String, cols: Column*): Unit = {
    option("path", path)
    IndexedDataSource(
      sparkSession,
      className = source,
      mode = mode,
      options = extraOptions.toMap).createIndex(cols)
  }

  def delete(path: String): Unit = {
    option("path", path)
    IndexedDataSource(
      sparkSession,
      className = source,
      mode = mode,
      options = extraOptions.toMap).deleteIndex()
  }

  private var mode: SaveMode = SaveMode.ErrorIfExists
  private var source: String = sparkSession.sessionState.conf.defaultDataSourceName
  private var extraOptions = new scala.collection.mutable.HashMap[String, String]
}
