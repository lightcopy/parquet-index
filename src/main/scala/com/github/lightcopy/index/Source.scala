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

package com.github.lightcopy.index

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame}

import org.slf4j.LoggerFactory

import com.github.lightcopy.{Catalog, IndexSpec, Util}
import com.github.lightcopy.index.simple.SimpleSource
import com.github.lightcopy.index.parquet.ParquetSource

/**
 * Internal loader for [[IndexSource]], and is used by [[InternalCatalog]]. Every available
 * implementation should be registered here. Source is file-system based, therefore root folder for
 * index is always created but conditional on successful index creation. Metadata is stored using
 * internal methods and should not be stored in implementation.
 */
private[lightcopy] object Source {
  private val logger = LoggerFactory.getLogger(getClass)
  val METADATA_FILE = "_index_metadata"
  // Parquet source
  val PARQUET = "parquet"
  // simple source for testing
  val SIMPLE = "simple"

  /** Resolve source, fail if source is unsupported */
  def resolveSource(source: String): IndexSource = source match {
    case SIMPLE => new SimpleSource()
    case PARQUET => new ParquetSource()
    case other => throw new UnsupportedOperationException(
      s"Source $other is not supported, accepted sources are '$SIMPLE', '$PARQUET'")
  }

  def metadataPath(root: Path): Path = {
    root.suffix(s"${Path.SEPARATOR}$METADATA_FILE")
  }

  /** Read index metadata */
  def readMetadata(fs: FileSystem, root: Path): Metadata = {
    SerDe.deserialize(Util.readContent(fs, metadataPath(root)))
  }

  /** Write index metadata */
  def writeMetadata(fs: FileSystem, root: Path, metadata: Metadata): Unit = {
    Util.writeContent(fs, metadataPath(root), SerDe.serialize(metadata))
  }

  /**
   * Create fresh index root directory and initialize index using provided closure.
   * If closure function throws exception directory is cleaned up automatically.
   */
  def withRootDirectoryForIndex(catalog: Catalog)(func: Path => Index): Index = {
    val rootDir = catalog.getFreshIndexDirectory()
    try {
      func(rootDir)
    } catch {
      case err: Throwable =>
        try {
          catalog.fs.delete(rootDir, true)
        } catch {
          case NonFatal(err) =>
            // in case directory cannot be deleted, log warning and do nothing
            logger.warn(s"Could not remove directory $rootDir for index, may require manual delete")
        }
        throw err
    }
  }

  /** Load index based on available metadata */
  def loadIndex(catalog: Catalog, status: FileStatus): Index = {
    val root = status.getPath
    val metadata = readMetadata(catalog.fs, root)
    logger.info(s"Found metadata $metadata for index in $root")
    resolveSource(metadata.source).loadIndex(catalog, metadata)
  }

  /** Create index based on source implementation */
  def createIndex(catalog: Catalog, indexSpec: IndexSpec, columns: Seq[Column]): Index = {
    // create folder for index, always store metadata after index creation to ensure consistency
    withRootDirectoryForIndex(catalog) { dir =>
      // modify index spec to include index directory
      indexSpec.setConf(IndexSpec.INDEX_DIR, dir.toString)
      val index = resolveSource(indexSpec.source).createIndex(catalog, indexSpec, columns)
      logger.info(s"Created index $index")
      writeMetadata(catalog.fs, dir, index.getMetadata)
      logger.info(s"Created metadata for index $index")
      index
    }
  }

  /** Use source implementation to apply fallback (normally just standard scan) */
  def fallback(catalog: Catalog, indexSpec: IndexSpec, condition: Column): DataFrame = {
    resolveSource(indexSpec.source).fallback(catalog, indexSpec, condition)
  }
}
