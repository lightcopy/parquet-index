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

import org.apache.spark.sql.{Column, DataFrame, SaveMode, SQLContext}

import org.slf4j.LoggerFactory

import com.github.lightcopy.index.{Index, Source}

/**
 * [[Catalog]] manages internal index metastore. It assumes that index metadata is stored on disk
 * despite actual data being stored either on disk or database.
 */
abstract class Catalog {

  /** File system for catalog */
  def fs: FileSystem

  /**
   * Get fully-qualified path to the backed metastore. Can be local file system or HDFS.
   */
  def metastorePath: String

  /**
   * Get new index root directory, should fail on collisions, e.g. directory already exists.
   */
  def getFreshIndexDirectory(): Path

  /**
   * List index directories available in metastore. Returns sequence of [[Index]],
   * which might have different underlying implementation, e.g. Parquet or ORC.
   */
  def listIndexes(): Seq[Index]

  /**
   * Find index based on provided [[IndexSpec]]. Should return None, if index does not exist, and
   * should fail if index is corrupt.
   * @param indexSpec partial index specification
   */
  def getIndex(indexSpec: IndexSpec): Option[Index]

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
   * Query index based on [[IndexSpec]] and condition. Internal behaviour should depend on
   * condition and supported predicates as well as amount of data returned. Note that fallback
   * strategy may not be supported by underlying index implementation.
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

/** Internal file system implementation of [[Catalog]] with support for caching */
class InternalCatalog(
    @transient val sqlContext: SQLContext)
  extends Catalog {

  private val logger = LoggerFactory.getLogger(getClass)

  private val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
  private val metastore = resolveMetastore(getMetastorePath(sqlContext), hadoopConf)
  override val fs = metastore.getFileSystem(hadoopConf)
  logger.info(s"Resolved metastore directory to $metastore")
  logger.info(s"Registered file system $fs")

  /** Resolve metastore path as raw string */
  private[lightcopy] def getMetastorePath(sqlContext: SQLContext): Option[String] = {
    Try(sqlContext.getConf(InternalCatalog.METASTORE_OPTION)).toOption
  }

  /** Return fully-qualified path for metastore */
  private[lightcopy] def resolveMetastore(rawPath: Option[String], conf: Configuration): Path = {
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
    status.getPath
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

  //////////////////////////////////////////////////////////////
  // == Catalog implementation ==
  //////////////////////////////////////////////////////////////

  override def metastorePath: String = metastore.toString

  /** Create fresh index directory, should not collide with any existing paths */
  override def getFreshIndexDirectory(): Path = {
    val uid = UUID.randomUUID.toString
    val dir = metastore.suffix(s"${Path.SEPARATOR}$uid")
    if (fs.exists(dir)) {
      throw new IllegalStateException(s"Fresh directory collision, directory $dir already exists")
    }
    fs.mkdirs(dir, InternalCatalog.METASTORE_PERMISSION)
    fs.resolvePath(dir)
  }

  override def listIndexes(): Seq[Index] = {
    Option(fs.listStatus(metastore)) match {
      case Some(statuses) if statuses.nonEmpty =>
        statuses.filter { _.isDirectory }.flatMap { status =>
          // index might fail to load, we wrap it into try-catch and log error
          try {
            Some(Source.loadIndex(this, status))
          } catch {
            case NonFatal(err) =>
              logger.debug(s"Failed to load index for status $status, reason=$err")
              None
          }
        }.toSeq
      case other => Seq.empty
    }
  }

  override def getIndex(indexSpec: IndexSpec): Option[Index] = {
    val found = listIndexes().filter { index => index.containsSpec(indexSpec) }
    found.headOption
  }

  override def createIndex(indexSpec: IndexSpec, columns: Seq[Column]): Unit = {
    // make decision on how to load index based on provided save mode
    val maybeIndex = getIndex(indexSpec)
    indexSpec.mode match {
      case SaveMode.ErrorIfExists =>
        if (maybeIndex.isDefined) {
          throw new IllegalStateException(s"Index already exists for spec $indexSpec")
        } else {
          Source.createIndex(this, indexSpec, columns)
        }
      case SaveMode.Overwrite =>
        if (maybeIndex.isDefined) {
          logger.info(s"Delete index for spec $indexSpec")
          // prepare index for deletion and drop what is left including metadata
          dropIndex(indexSpec)
        }
        Source.createIndex(this, indexSpec, columns)
      case SaveMode.Append =>
        // this really depends on index implementation support for append
        if (maybeIndex.isDefined) {
          logger.info(s"Append to existing index for spec $indexSpec")
          maybeIndex.get.append(indexSpec, columns)
        } else {
          Source.createIndex(this, indexSpec, columns)
        }
      case SaveMode.Ignore =>
        // no-op if index exists, log the action
        if (maybeIndex.isDefined) {
          logger.info(s"Index exists for spec $indexSpec, ignore 'createIndex'")
        } else {
          Source.createIndex(this, indexSpec, columns)
        }
    }
  }

  override def dropIndex(indexSpec: IndexSpec): Unit = {
    getIndex(indexSpec) match {
      case Some(index) =>
        val path = new Path(index.getRoot)
        logger.info(s"Delete index $index at root $path")
        index.delete()
        if (!fs.delete(path, true)) {
          logger.warn(s"Failed to delete index $index for path $path, may require manual cleanup")
        }
      case None => // do nothing
    }
  }

  override def queryIndex(indexSpec: IndexSpec, condition: Column): DataFrame = {
    getIndex(indexSpec) match {
      case Some(index) =>
        logger.info(s"Search index $index for $condition")
        index.search(condition)
      case None =>
        logger.warn(s"Index for spec $indexSpec is not found, using fallback strategy")
        Source.fallback(this, indexSpec, condition)
    }
  }
}

private[lightcopy] object InternalCatalog {
  // reserved Spark configuration option for metastore
  val METASTORE_OPTION = "spark.sql.index.metastore"
  // default metastore directory name
  val DEFAULT_METASTORE_DIR = "index_metastore"
  // permission mode "rw-rw-rw-"
  val METASTORE_PERMISSION =
    new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE)
}
