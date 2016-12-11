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

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class Metastore(@transient val session: SparkSession) extends Logging {
  private val hadoopConf = session.sparkContext.hadoopConfiguration
  private val metastore = resolveMetastore(getMetastorePath(session), hadoopConf)
  private val fs = metastore.getFileSystem(hadoopConf)
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
}
