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

import scala.collection.mutable.{HashMap => MutableHashMap}

import org.apache.spark.sql.execution.datasources.{IndexedDataSource, Metastore}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

/**
 * Entrypoint for working with index functionality, e.g. reading indexed table, creating index
 * for provided file path, or deleting index for table.
 */
class DataFrameIndexManager(sparkSession: SparkSession) {
  private var source: String = IndexedDataSource.parquet
  private var extraOptions = new MutableHashMap[String, String]()

  /** File format for table */
  def format(source: String): DataFrameIndexManager = {
    this.source = source
    this
  }

  /** Add key-value to options */
  def option(key: String, value: String): DataFrameIndexManager = {
    this.extraOptions += (key -> value)
    this
  }

  /** Add boolean value to options, for compatibility with Spark */
  def option(key: String, value: Boolean): DataFrameIndexManager = {
    option(key, value.toString)
  }

  /** Add long value to options, for compatibility with Spark */
  def option(key: String, value: Long): DataFrameIndexManager = {
    option(key, value.toString)
  }

  /** Add double value to options, for compatibility with Spark */
  def option(key: String, value: Double): DataFrameIndexManager = {
    option(key, value.toString)
  }

  /** Add options from external map */
  def options(options: scala.collection.Map[String, String]): DataFrameIndexManager = {
    this.extraOptions ++= options
    this
  }

  /**
   * Load indexed table as DataFrame.
   * @param path filepath to the table (directory)
   */
  def load(path: String): DataFrame = {
    option("path", path)
    sparkSession.baseRelationToDataFrame(
      IndexedDataSource(
        Metastore.getOrCreate(sparkSession),
        className = source,
        options = extraOptions.toMap).resolveRelation())
  }

  /**
   * Load indexed DataFrame from Parquet table.
   * @param path filepath to the Parquet table (directory)
   */
  def parquet(path: String): DataFrame = {
    format(IndexedDataSource.parquet).load(path)
  }

  /** DDL command to create index for provided source with options */
  def create: CreateIndexCommand = {
    CreateIndexCommand(
      sparkSession = sparkSession,
      source = source,
      options = extraOptions)
  }

  /** DDL command to delete index for provided source */
  def delete: DeleteIndexCommand = {
    DeleteIndexCommand(
      sparkSession = sparkSession,
      source = source,
      options = extraOptions)
  }
}

/**
 * [[CreateIndexCommand]] provides functionality to create index for a table. Requires index
 * columns and valid table path. Also allows to specify different mode for creating index, similar
 * to writing DataFrame.
 */
private[sql] case class CreateIndexCommand(
    @transient val sparkSession: SparkSession,
    private var source: String,
    private var options: MutableHashMap[String, String]) {
  private var mode: SaveMode = SaveMode.ErrorIfExists
  private var columns: Seq[Column] = Nil

  /**
   * Provide mode for creating index.
   * @param mode save mode
   */
  def mode(mode: SaveMode): CreateIndexCommand = {
    this.mode = mode
    this
  }

  /**
   * Provide string-like mode to create index.
   * @param mode string value for save mode
   */
  def mode(mode: String): CreateIndexCommand = {
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

  /** Specify columns to index, at least one column is required */
  def indexBy(column: Column, columns: Column*): CreateIndexCommand = {
    this.columns = column +: columns
    this
  }

  /** Specify column names to index, at least one column is required */
  def indexBy(columnName: String, columnNames: String*): CreateIndexCommand = {
    indexBy(col(columnName), columnNames.map(col): _*)
  }

  /**
   * Path to the table to build index for, can be local file system or HDFS.
   * @param path
   */
  def table(path: String): Unit = {
    this.options += "path" -> path
    IndexedDataSource(
      Metastore.getOrCreate(sparkSession),
      className = source,
      mode = mode,
      options = this.options.toMap).createIndex(this.columns)
  }

  /** Path to the parquet table, forces source to be Parquet */
  def parquet(path: String): Unit = {
    this.source = IndexedDataSource.parquet
    table(path)
  }
}

/**
 * [[DeleteIndexCommand]] provides functionality to delete existing index. Current behaviour is
 * no-op when deleting non-existent index.
 */
private[sql] case class DeleteIndexCommand(
    @transient val sparkSession: SparkSession,
    private var source: String,
    private val options: MutableHashMap[String, String]) {

  /** Path to the table to delete index for */
  def table(path: String): Unit = {
    this.options += "path" -> path
    IndexedDataSource(
      Metastore.getOrCreate(sparkSession),
      className = source,
      options = this.options.toMap).deleteIndex()
  }

  /** Path to the parquet table to delete index for, forces Parquet source */
  def parquet(path: String): Unit = {
    this.source = IndexedDataSource.parquet
    table(path)
  }
}
