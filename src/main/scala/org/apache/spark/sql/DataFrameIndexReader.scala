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

  def schema(schema: StructType): DataFrameIndexReader = {
    this.userSpecifiedSchema = Option(schema)
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

  def load(): DataFrame = {
    load(Seq.empty: _*)
  }

  def load(path: String): DataFrame = {
    option("path", path).load(Seq.empty: _*)
  }

  def load(paths: String*): DataFrame = {
    sparkSession.baseRelationToDataFrame(
      IndexedDataSource(
        sparkSession,
        paths = paths,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = extraOptions.toMap).resolveRelation())
  }

  /**
   * Loads a Parquet file, returning the result as a `DataFrame`. See the documentation
   * on the other overloaded `parquet()` method for more details.
   *
   * @since 2.0.0
   */
  def parquet(path: String): DataFrame = {
    format("parquet").load(Seq(path): _*)
  }

  private var source: String = sparkSession.sessionState.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]
}
