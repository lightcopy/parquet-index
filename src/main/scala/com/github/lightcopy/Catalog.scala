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

import java.util.UUID

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}

import org.apache.spark.sql.{Column, DataFrame, SQLContext}

import org.slf4j.LoggerFactory

/**
 * [[Catalog]] provides access to internal index metastore.
 */
abstract class Catalog {

  /**
   * Get fully-qualified path to the backed metastore. Can be local file system or HDFS.
   */
  def metastorePath: String

  /**
   * List index directories available in metastore. Returns sequence of [[IndexStatus]],
   * which might have different underlying implementation, e.g. Parquet or ORC.
   */
  def listIndexes(): Seq[IndexStatus]

  /**
   * Create index based on partial information as [[IndexSpec]] and set of columns. Should check if
   * index can be created and apply certain action based on save mode.
   * @param indexSpec partial index specification
   * @param columns list of columns to index
   */
  def createIndex(indexSpec: IndexSpec, columns: Seq[Column]): Unit

  /**
   * Drop index based on [[IndexSpec]], note that if no index matches spec, results in no-op.
   * @param indexSpec partial index specification
   */
  def dropIndex(indexSpec: IndexSpec): Unit

  /**
   * Query index based on [[IndexSpec]] and condition. Internal behaviour should depend on condition
   * and supported predicates as well as amount of data returned.
   * @param indexSpec partial index specification
   * @param condition filter expression to use
   */
  def queryIndex(indexSpec: IndexSpec, condition: Column): DataFrame

  /**
   * Refresh in-memory cache if supported by catalog.
   * Should invalidate cache and reload from metastore.
   * @param indexSpec partial index specification
   */
  def refreshIndex(indexSpec: IndexSpec): Unit = { }
}

/** Internal implementation of [[Catalog]] with support for in-memory index */
class InternalCatalog(
    @transient val sqlContext: SQLContext)
  extends Catalog {

  private val logger = LoggerFactory.getLogger(getClass)

  private val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
  private val metastore = resolveMetastore(getMetastorePath(sqlContext), hadoopConf)
  logger.info(s"Resolved metastore directory to $metastore")

  /** Resolve metastore path as raw string */
  private[lightcopy] def getMetastorePath(sqlContext: SQLContext): Option[String] = {
    Try(sqlContext.getConf(InternalCatalog.METASTORE_OPTION)).toOption
  }

  /** Return fully-qualified path for metastore */
  private[lightcopy] def resolveMetastore(rawPath: Option[String], conf: Configuration): String = {
    val hadoopConf = if (rawPath.isEmpty) new Configuration(false) else conf
    val path = new Path(rawPath.getOrElse(InternalCatalog.DEFAULT_METASTORE_DIR))
    val fs = path.getFileSystem(hadoopConf)

    // create directory with read/write permissions, if does not exist
    if (!fs.exists(path)) {
      val permission = InternalCatalog.METASTORE_PERMISSION
      logger.info(s"Creating metastore directory($permission) for $path")
      fs.mkdirs(path, permission)
    }

    val status = fs.getFileStatus(path)
    validateMetastoreStatus(status)
    status.getPath.toString
  }

  /** Validate metastore status, throw exception if parameters do not match */
  private[lightcopy] def validateMetastoreStatus(status: FileStatus): Unit = {
    // status is a directory with read/write access
    if (!status.isDirectory) {
      throw new IllegalStateException(s"Expected directory for metastore, found ${status.getPath}")
    }

    val permission = status.getPermission
    val expected = InternalCatalog.METASTORE_PERMISSION
    val readWriteAccess =
      permission.getUserAction.implies(expected.getUserAction) &&
      permission.getGroupAction.implies(expected.getGroupAction) &&
      permission.getOtherAction.implies(expected.getOtherAction)

    if (!readWriteAccess) {
      throw new IllegalStateException(
        s"Expected directory with $expected, found ${status.getPath}($permission)")
    }
  }

  /** Path filter for index metadata files */
  private[lightcopy] def metadataPathFilter: PathFilter = {
    new PathFilter() {
      override def accept(path: Path): Boolean = {
        path.getName == InternalCatalog.METASTORE_INDEX_METADATA_FILE
      }
    }
  }

  /** Convert file status into index status */
  private[lightcopy] def fsToIndexStatus(
      fs: FileSystem,
      status: FileStatus): Option[IndexStatus] = {
    if (status.isDirectory) {
      val metadata = fs.listStatus(status.getPath, metadataPathFilter).headOption
      if (metadata.isDefined) {
        var inputStream = fs.open(metadata.get.getPath)
        try {
          Some(IndexStatus(status.getPath.getName, status.getPath.toString, inputStream))
        } finally {
          if (inputStream != null) {
            inputStream.close()
          }
        }
      } else {
        None
      }
    } else {
      None
    }
  }

  override def metastorePath: String = metastore

  override def listIndexes(): Seq[IndexStatus] = {
    val path = new Path(metastorePath)
    val fs = path.getFileSystem(hadoopConf)
    val statuses = Option(fs.listStatus(path))
    if (statuses.isDefined) {
      statuses.get.filter { _.isDirectory }.flatMap { status =>
        fsToIndexStatus(fs, status) }.toSeq
    } else {
      Seq.empty
    }
  }

  override def createIndex(indexSpec: IndexSpec, columns: Seq[Column]): Unit = {
    createIndex(UUID.randomUUID.toString, indexSpec, columns)
  }

  private[lightcopy] def createIndex(
      indexName: String,
      indexSpec: IndexSpec,
      columns: Seq[Column]): Unit = {
    require(columns.nonEmpty, "Expected non-empty list of columns to create index")
    val indexPath = new Path(metastorePath).suffix(s"${Path.SEPARATOR}/$indexName")
    logger.info(s"Creating index $indexName in $indexPath")
    val fs = indexPath.getFileSystem(hadoopConf)
    if (fs.exists(indexPath)) {
      throw new IllegalStateException(s"Index path $indexPath already exists")
    }
    if (!fs.mkdirs(indexPath)) {
      throw new IllegalStateException(s"Failed to create directory $indexPath")
    }

    val resolvedColumns = resolveColumnNames(columns)
    val metadataPath = indexPath.suffix(
      s"${Path.SEPARATOR}/${InternalCatalog.METASTORE_INDEX_METADATA_FILE}")
    var outputStream = fs.create(metadataPath)
    try {
      IndexStatus.create(indexName, indexPath.toString, indexSpec, resolvedColumns, outputStream)
    } catch {
      case NonFatal(err) =>
        fs.delete(indexPath, true)
        throw err
    } finally {
      if (outputStream != null) {
        outputStream.close()
      }
    }
  }

  /** Convert columns into column names */
  private def resolveColumnNames(columns: Seq[Column]): Seq[String] = {
    Seq.empty
  }

  override def dropIndex(indexSpec: IndexSpec): Unit = {
    val indexes = listIndexes()
    for (index <- indexes) {
      if (index.equalsSpec(indexSpec)) {
        val path = new Path(index.getRootPath)
        logger.info(s"Delete index $index at root $path")
        val fs = path.getFileSystem(hadoopConf)
        if (!fs.delete(path, true)) {
          logger.warn(s"Failed to delete index $index")
        }
      }
    }
  }

  override def queryIndex(indexSpec: IndexSpec, condition: Column): DataFrame = {
    null
  }

  override def refreshIndex(indexSpec: IndexSpec): Unit = { }
}

private[lightcopy] object InternalCatalog {
  // reserved Spark configuration option for metastore
  val METASTORE_OPTION = "spark.sql.index.metastore"
  // default metastore directory name
  val DEFAULT_METASTORE_DIR = "index_metastore"
  // permission mode "rw-rw-rw-"
  val METASTORE_PERMISSION =
    new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE)
  // name for metadata file for each index
  val METASTORE_INDEX_METADATA_FILE = "_index_metadata"
}
