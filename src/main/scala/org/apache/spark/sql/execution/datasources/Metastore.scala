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
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, Cache, RemovalListener, RemovalNotification}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.internal.IndexConf

/**
 * [[Metastore]] provides global access to index directory and allows to locate index directory
 * when creating, loading or deleting table index.
 * @param session Spark session
 * @param conf index configuration
 */
private[sql] class Metastore(
    @transient val session: SparkSession,
    val conf: IndexConf) extends Logging {
  // Hadoop configuration used to create Spark context, to capture default file system
  private val hadoopConf = session.sparkContext.hadoopConfiguration
  // Metastore location to use
  private val metastore = resolveMetastore(getMetastorePath, hadoopConf)
  // Publicly available file system that is used to metastore
  val fs = metastore.getFileSystem(hadoopConf)

  logInfo(s"Resolved metastore directory to $metastore")
  logInfo(s"Registered file system $fs")

  // cache of index catalogs per metastore
  val onRemovalAction = new RemovalListener[Path, MetastoreIndexCatalog] {
    override def onRemoval(rm: RemovalNotification[Path, MetastoreIndexCatalog]): Unit = {
      logInfo(s"Evicting index ${rm.getKey}")
    }
  }

  val cache: Cache[Path, MetastoreIndexCatalog] =
    CacheBuilder.newBuilder().
      maximumSize(16).
      expireAfterWrite(12, TimeUnit.HOURS).
      removalListener(onRemovalAction).
      build()

  logInfo(s"Registered cache $cache")

  /** Resolve metastore path as raw string */
  private[sql] def getMetastorePath: Option[String] = {
    Option(conf.metastoreLocation) match {
      case valid @ Some(value) if value.nonEmpty => valid
      case other => None
    }
  }

  /** Return fully-qualified path for metastore */
  private[sql] def resolveMetastore(rawPath: Option[String], conf: Configuration): Path = {
    // path is not provided always resolve to local file system
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
   *
   * Closure provides two parameters:
   * - path, file status to the index directory, exists when closure is called
   * - isAppend, boolean flag indicating that directory already contains files for current index and
   * format needs to append new data to the existing files
   *
   * Cache is invalidated for index every time `create` method is called for that index.
   */
  def create(identifier: String, path: Path, mode: SaveMode)
      (func: (FileStatus, Boolean) => Unit): Unit = {
    // reconstruct path with metastore path as root directory
    val resolvedPath = location(identifier, path)
    cache.invalidate(resolvedPath)
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
          throw err
      }
    }
  }

  /**
   * Load index directory if exists, fail if none found for identifier.
   * If resolved path exists in memory, load from metastore cache, otherwise read from disk and
   * update cache. It still works in situation when flag is enabled to create index if one does not
   * exist, because if index exists, we just load it from cache, otherwise we create fresh index
   * that is definitely not in the cache yet.
   * Note that is this behaviour changes, cache should be updated accordingly.
   */
  def load(identifier: String, path: Path)
      (func: FileStatus => MetastoreIndexCatalog): MetastoreIndexCatalog = {
    val resolvedPath = location(identifier, path)
    Option(cache.getIfPresent(resolvedPath)) match {
      case Some(cachedValue) =>
        logInfo(s"Loading $identifier[$resolvedPath] from cache")
        cachedValue
      case None =>
        if (!fs.exists(resolvedPath)) {
          throw new IOException(s"Index does not exist for $identifier and path $path")
        }
        val loadedValue = func(fs.getFileStatus(resolvedPath))
        cache.put(resolvedPath, loadedValue)
        loadedValue
    }
  }

  /**
   * Delete index directory, exposes function to clean up directory when special handling of files
   * in that directory is required. Cache is invalidated only if index already exists for resolved
   * path, otherwise no-op.
   */
  def delete(identifier: String, path: Path)(func: FileStatus => Unit): Unit = {
    val resolvedPath = location(identifier, path)
    if (fs.exists(resolvedPath)) {
      try {
        cache.invalidate(resolvedPath)
        func(fs.getFileStatus(resolvedPath))
      } finally {
        try {
          fs.delete(resolvedPath, true)
          logInfo(s"Deleted index path $resolvedPath for $identifier")
        } catch {
          case NonFatal(io) =>
            logWarning(s"Failed to delete path $resolvedPath for $identifier", io)
        }
      }
    }
  }

  /**
   * Check whether or not location exists for given identifier and path.
   * This method should bypass cache, because we check directly in metastore if index exists, and
   * do not invalidate cache otherwise.
   */
  def exists(identifier: String, path: Path): Boolean = {
    val resolvedPath = location(identifier, path)
    fs.exists(resolvedPath)
  }

  /**
   * Get path of index location for an identifier and path. Path is expected to be fully-qualified
   * filepath with scheme.
   */
  private[datasources] def location(identifier: String, path: Path): Path = {
    Metastore.validateSupportIdentifier(identifier)
    val scheme = Option(path.toUri.getScheme).getOrElse(fs.getScheme)
    metastore.
      suffix(s"${Path.SEPARATOR}$identifier").
      suffix(s"${Path.SEPARATOR}$scheme").
      suffix(s"${Path.SEPARATOR}${path.toUri.getPath}")
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}[metastore=$metastoreLocation]"
  }
}

object Metastore {
  // default metastore directory name
  val DEFAULT_METASTORE_DIR = "index_metastore"
  // permission mode "rwxrw-rw-"
  val METASTORE_PERMISSION =
    new FsPermission(FsAction.ALL, FsAction.READ_WRITE, FsAction.READ_WRITE)

  private val stores = scala.collection.mutable.HashMap[SparkSession, Metastore]()

  /**
   * Get already cached metastore for session, or create new one with index configuration.
   * Index configuration is not updated when cached version is used.
   */
  def getOrCreate(sparkSession: SparkSession): Metastore = {
    stores.getOrElseUpdate(sparkSession, new Metastore(sparkSession,
      IndexConf.newConf(sparkSession)))
  }

  def validateSupportIdentifier(identifier: String): Unit = {
    require(identifier != null && identifier.nonEmpty, "Empty invalid identifier")
    identifier.foreach { ch =>
      require(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z', s"Invalid character $ch in " +
        s"identifier $identifier. Only lowercase alpha-numeric characters are supported")
    }
  }
}
