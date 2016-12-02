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

import java.io.FileNotFoundException

import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}

import org.apache.spark.sql.Column

import com.github.lightcopy.{Catalog, IndexSpec}

/**
 * [[FileSource]] is a file-system based partial implementation of abstract [[IndexSource]],
 * specifically tailored to work with HDFS.
 */
abstract class FileSource extends IndexSource {

  /**
   * Load file index.
   * @param catalog current catalog
   * @param metadata preloaded index metadata
   */
  def loadFileIndex(catalog: Catalog, metadata: Metadata): Index

  /**
   * Create file index based on resolved root index directory and resolved paths for datasource,
   * each path points to a file under datasource directory/path. It is not recommended to rely on
   * paths being direct children of 'tablePath', since it might include directory partitioning.
   * Column names are converted be checked against files' schema.
   * @param catalog current catalog
   * @param indexDir root index directory
   * @param tablePath fully-resolved path to the table (global parent directory for paths)
   * @param paths non-empty list of datasource files to process (after applied filtering)
   * @param colNames non-empty list of unique column string names
   * @return file index
   */
  def createFileIndex(
      catalog: Catalog,
      indexDir: FileStatus,
      tablePath: FileStatus,
      paths: Array[FileStatus],
      colNames: Seq[String]): Index

  /**
   * Provide custom filter to keep paths that relevant to the index source, e.g. removing commit
   * files or fetch compressed files only. By default returns all files found.
   * @return hadoop path filter
   */
  def pathFilter(): PathFilter = new PathFilter() {
    override def accept(path: Path): Boolean = true
  }

  //////////////////////////////////////////////////////////////
  // == Internal implementation ==
  //////////////////////////////////////////////////////////////

  override def loadIndex(catalog: Catalog, metadata: Metadata): Index = {
    loadFileIndex(catalog, metadata)
  }

  override def createIndex(catalog: Catalog, spec: IndexSpec, columns: Seq[Column]): Index = {
    // since it is file system based index, we need to make sure that path is provided to store
    // index information including metadata, this path should be available as part of spec options
    val indexDir = discoverPath(catalog, spec.getConf(IndexSpec.INDEX_DIR))
    // parse column names, column names are always distinct
    val colNames = columns.map(withColumnName).distinct
    require(colNames.nonEmpty, s"Expected at least one column, found $columns")
    // parse index spec and resolve provided path
    val datasourcePath = spec.path.getOrElse(
      sys.error(s"$spec does not contain path that is required for file-system based index"))
    val datasourceStatus = discoverPath(catalog, datasourcePath)
    val statuses = listStatuses(catalog, datasourceStatus)
    require(statuses.nonEmpty, s"Expected at least one datasource file in $datasourcePath")
    createFileIndex(catalog, indexDir, datasourceStatus, statuses, colNames)
  }

  /** Convert Spark SQL column expressions into column names */
  private[lightcopy] def withColumnName(col: Column): String = col.toString

  /**
   * List files using datasource status. See `discoverPath(...)` method to check path requirements.
   * FileStatus should point to logical table on disk, e.g. Parquet table stored using Spark SQL.
   * Directory partitioning is not supported for now, e.g. search one level down from root.
   */
  private[lightcopy] def listStatuses(catalog: Catalog, root: FileStatus): Array[FileStatus] = {
    if (root.isDirectory) {
      catalog.fs.listStatus(root.getPath, pathFilter()).filter { _.isFile }
    } else {
      Array(root)
    }
  }

  /**
   * Resolve root file path for table. Path must be a valid path resolving to a single entry, e.g.
   * should not contain "*" if there is paths' ambiguity. Also it is not recommended to have it as
   * symlink.
   */
  private[lightcopy] def discoverPath(catalog: Catalog, path: String): FileStatus = {
    try {
      catalog.fs.getFileStatus(new Path(path))
    } catch {
      case exc: FileNotFoundException =>
        throw new FileNotFoundException(s"Datasource path $path cannot be resolved, " +
          "make sure that path points to either single file or directory without " +
          "regular expressions or globstars")
    }
  }
}
