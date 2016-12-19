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

package org.apache.spark.sql.execution.datasources

import scala.util.{Try, Success, Failure}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.parquet.ParquetIndexFileFormat
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/** DataSource to resolve relations that support indexing */
case class IndexedDataSource(
    metastore: Metastore,
    className: String,
    mode: SaveMode = SaveMode.ErrorIfExists,
    options: Map[String, String] = Map.empty,
    bucketSpec: Option[BucketSpec] = None) extends Logging {

  lazy val providingClass: Class[_] = IndexedDataSource.lookupDataSource(className)
  lazy val tablePath: FileStatus = {
    val path = options.getOrElse("path", sys.error("path option is required"))
    IndexedDataSource.resolveTablePath(new Path(path),
      metastore.session.sparkContext.hadoopConfiguration)
  }

  def resolveRelation(): BaseRelation = {
    val caseInsensitiveOptions = new CaseInsensitiveMap(options)
    providingClass.newInstance() match {
      case s: MetastoreSupport =>
        logInfo(s"Loading index for $s, table=${tablePath.getPath}")

        var indexCatalog = metastore.load(s.identifier, tablePath.getPath) { status =>
          s.loadIndex(metastore, status)
        }

        HadoopFsRelation(
          location = indexCatalog,
          partitionSchema = indexCatalog.partitionSpec().partitionColumns,
          dataSchema = indexCatalog.dataSchema().asNullable,
          bucketSpec = bucketSpec,
          fileFormat = s.fileFormat,
          options = caseInsensitiveOptions)(metastore.session)
      case other =>
        throw new UnsupportedOperationException(s"Index is not supported by $other")
    }
  }

  /**
   * Create index based on provided format, if no exception is thrown during creation, considered
   * as process succeeded.
   */
  def createIndex(columns: Seq[Column]): Unit = providingClass.newInstance() match {
    case s: MetastoreSupport =>
      logInfo(s"Create index for $s, table=${tablePath.getPath}, columns=$columns, mode=$mode")
      // infer partitions from file path
      val paths = Seq(tablePath.getPath)
      val partitionSchema: Option[StructType] = None
      val catalog = new ListingFileCatalog(metastore.session, paths, options, partitionSchema,
        ignoreFileNotFound = false)
      val partitionSpec = catalog.partitionSpec
      // ignore filtering expression for partitions, fetch all available files
      val allFiles = catalog.listFiles(Nil)
      metastore.create(s.identifier, tablePath.getPath, mode) { (status, isAppend) =>
        s.createIndex(metastore, status, tablePath, isAppend, partitionSpec, allFiles, columns)
      }
    case other =>
      throw new UnsupportedOperationException(s"Creation of index is not supported by $other")
  }

  /** Delete index if exists, otherwise no-op */
  def deleteIndex(): Unit = providingClass.newInstance() match {
    case s: MetastoreSupport =>
      logInfo(s"Delete index for $s, table=${tablePath.getPath}")
      metastore.delete(s.identifier, tablePath.getPath) { case status =>
        s.deleteIndex(metastore, status) }
    case other =>
      throw new UnsupportedOperationException(s"Deletion of index is not supported by $other")
  }
}

object IndexedDataSource {
  val parquet = classOf[ParquetIndexFileFormat].getCanonicalName

  /**
   * Resolve class name into fully-qualified class path if available. If no match found, return
   * itself. [[IndexedDataSource]] checks whether or not class is a valid indexed source.
   */
  def resolveClassName(provider: String): String = provider match {
    case "parquet" => parquet
    case "org.apache.spark.sql.execution.datasources.parquet" => parquet
    case other => other
  }

  /** Simplified version of looking up datasource class */
  def lookupDataSource(provider: String): Class[_] = {
    val provider1 = IndexedDataSource.resolveClassName(provider)
    val provider2 = s"$provider.DefaultSource"
    val loader = Utils.getContextOrSparkClassLoader
    Try(loader.loadClass(provider1)).orElse(Try(loader.loadClass(provider1))) match {
      case Success(dataSource) =>
        dataSource
      case Failure(error) =>
        throw new ClassNotFoundException(
          s"Failed to find data source: $provider", error)
    }
  }

  /** Resolve table path into file status, should not contain any glob expansions */
  def resolveTablePath(path: Path, conf: Configuration): FileStatus = {
    val fs = path.getFileSystem(conf)
    fs.getFileStatus(path)
  }
}
