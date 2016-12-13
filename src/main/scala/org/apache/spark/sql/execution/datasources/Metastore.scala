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

import java.io.IOException

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

class Metastore(@transient val session: SparkSession) extends Logging {
  private val hadoopConf = session.sparkContext.hadoopConfiguration
  private val metastore = resolveMetastore(getMetastorePath(session), hadoopConf)
  val fs = metastore.getFileSystem(hadoopConf)
  logInfo(s"Resolved metastore directory to $metastore")
  logInfo(s"Registered file system $fs")

  /** Resolve metastore path as raw string */
  private[sql] def getMetastorePath(session: SparkSession): Option[String] = {
    Try(session.conf.get(Metastore.METASTORE_OPTION)).toOption
  }

  /** Return fully-qualified path for metastore */
  private[sql] def resolveMetastore(rawPath: Option[String], conf: Configuration): Path = {
    val hadoopConf = if (rawPath.isEmpty) new Configuration(false) else conf
    val path = new Path(rawPath.getOrElse(Metastore.DEFAULT_METASTORE_DIR))
    val fs = path.getFileSystem(hadoopConf)
    // create directory with read/write permissions, if does not exist
    if (!fs.exists(path)) {
      val permission = Metastore.METASTORE_PERMISSION
      logInfo(s"Creating metastore directory ($permission) for $path")
      if (!fs.mkdirs(path, permission)) {
        logWarning(s"Could not create metastore directory $path")
      }
    }

    val status = fs.getFileStatus(path)
    validateMetastoreStatus(status)
    status.getPath
  }

  /** Validate metastore status, throw exception if parameters do not match */
  private[sql] def validateMetastoreStatus(status: FileStatus): Unit = {
    // status is a directory with read/write access
    if (!status.isDirectory) {
      throw new IllegalStateException(s"Expected directory for metastore, found ${status.getPath}")
    }

    val permission = status.getPermission
    val expected = Metastore.METASTORE_PERMISSION
    val readWriteAccess =
      permission.getUserAction.implies(expected.getUserAction) &&
      permission.getGroupAction.implies(expected.getGroupAction) &&
      permission.getOtherAction.implies(expected.getOtherAction)

    if (!readWriteAccess) {
      throw new IllegalStateException(
        s"Expected directory with $expected, found ${status.getPath}($permission)")
    }
  }

  def metastoreLocation: String = metastore.toString

  /**
   * Create target directory for specific table path and index identifier. If index fails to create
   * target directory is deleted. Method also supports save mode, and propagates boolean flag based
   * on mode to the closure.
   * Closure provides two parameters:
   * - path, file status to the index directory, exists when closure is called
   * - isAppend, boolean flag indicating that directory already contains files for current index and
   * format needs to append new data to the existing files
   */
  def create(identifier: String, path: Path, mode: SaveMode)
      (func: (FileStatus, Boolean) => Unit): Unit = {
    // reconstruct path with metastore path as root directory
    val resolvedPath = location(identifier, path)
    val pathExists = fs.exists(resolvedPath)
    val (continue, isAppend) = mode match {
      case SaveMode.Append =>
        (true, pathExists)
      case SaveMode.Overwrite =>
        // delete directory
        if (pathExists && !fs.delete(resolvedPath, true)) {
          throw new IOException(s"Failed to delete path $resolvedPath, mode=$mode")
        }
        (true, false)
      case SaveMode.ErrorIfExists =>
        if (pathExists) {
          throw new IOException(s"Path $resolvedPath already exists, mode=$mode")
        }
        (true, false)
      case SaveMode.Ignore =>
        if (pathExists) {
          // log message and no-op
          logInfo(s"Path $resolvedPath as index directory already exists, mode=$mode")
        }
        (!pathExists, false)
    }

    if (continue) {
      try {
        fs.mkdirs(resolvedPath, Metastore.METASTORE_PERMISSION)
        func(fs.getFileStatus(resolvedPath), isAppend)
      } catch {
        case NonFatal(err) =>
          // delete path only if it is not append
          if (!isAppend) {
            try {
              fs.delete(resolvedPath, true)
            } catch {
              case NonFatal(io) =>
                logWarning(s"Failed to delete path $resolvedPath", io)
            }
          }
      }
    }
  }

  /**
   * Delete index directory, exposes function to clean up directory when special handling of files
   * in that directory is required.
   */
  def delete(identifier: String, path: Path)(func: FileStatus => Unit): Unit = {
    val resolvedPath = location(identifier, path)
    val pathExists = fs.exists(resolvedPath)
    if (pathExists) {
      try {
        func(fs.getFileStatus(resolvedPath))
      } finally {
        try {
          fs.delete(resolvedPath, true)
        } catch {
          case NonFatal(io) =>
            logWarning(s"Failed to delete path $resolvedPath", io)
        }
      }
    }
  }

  def location(identifier: String, path: Path): Path = {
    Metastore.validateSupportIdentifier(identifier)
    metastore.suffix(s"${Path.SEPARATOR}$identifier").
      suffix(s"${Path.SEPARATOR}${path.toUri.getPath}")
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}[metastore=$metastoreLocation]"
  }
}

private[sql] object Metastore {
  // reserved Spark configuration option for metastore
  val METASTORE_OPTION = "spark.sql.index.metastore"
  // default metastore directory name
  val DEFAULT_METASTORE_DIR = "index_metastore"
  // permission mode "rwxrw-rw-"
  val METASTORE_PERMISSION =
    new FsPermission(FsAction.ALL, FsAction.READ_WRITE, FsAction.READ_WRITE)

  private val stores = scala.collection.mutable.HashMap[SparkSession, Metastore]()

  def getOrCreate(sparkSession: SparkSession): Metastore = {
    stores.getOrElseUpdate(sparkSession, new Metastore(sparkSession))
  }

  def validateSupportIdentifier(identifier: String): Unit = {
    require(identifier != null && identifier.nonEmpty, "Empty invalid identifier")
    identifier.foreach { ch =>
      require(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z', s"Invalid character $ch in " +
        s"identifier $identifier. Only lowercase alpha-numeric characters are supported")
    }
  }
}
