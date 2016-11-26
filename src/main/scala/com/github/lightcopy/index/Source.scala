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

import org.apache.hadoop.fs.Path

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame}

import org.json4s.{DefaultFormats => JsonDefaultFormats}
import org.json4s.jackson.{JsonMethods => Json}

import com.github.lightcopy.{Catalog, IndexSpec, Util}
import com.github.lightcopy.index.parquet.ParquetSource

/**
 * Internal loader for [[IndexSource]], every available implementation should be registered here.
 */
private[lightcopy] object Source {
  val METADATA_FILE = "_index_metadata"
  val PARQUET = "parquet"
  implicit val formats = JsonDefaultFormats

  /** Resolve source, fail if source is unsupported */
  def resolveSource(source: String): IndexSource = source match {
    case PARQUET => new ParquetSource()
    case other => throw new UnsupportedOperationException(
      s"Source $other is not supported, accepted sources are '$PARQUET'")
  }

  /** Read index metadata */
  def readMetadata(fs: FileSystem, root: Path): Metadata = {
    val metadataPath = root.suffix(s"${Path.SEPARATOR}$METADATA_FILE")
    Json.parse(Util.readContent(fs, metadataPath)).extract[Metadata]
  }

  /** Load index based on available metadata */
  def loadIndex(catalog: Catalog, name: String, root: String): Index = {
    val path = new Path(root)
    val status = catalog.fs.getFileStatus(path)
    require(status.isDirectory, s"Expected directory, but found $root")
    val metadata = readMetadata(catalog.fs, status.getPath)
    resolveSource(metadata.source).loadIndex(catalog, name, root, Some(metadata))
  }

  /** Create index based on source implementation */
  def createIndex(catalog: Catalog, indexSpec: IndexSpec, columns: Seq[Column]): Index = {
    resolveSource(indexSpec.source).createIndex(catalog, indexSpec, columns)
  }

  /** Use source implementation to apply fallback (normally just standard scan) */
  def fallback(catalog: Catalog, indexSpec: IndexSpec, condition: Column): DataFrame = {
    resolveSource(indexSpec.source).fallback(catalog, indexSpec, condition)
  }
}
