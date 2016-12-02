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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{Column, SaveMode}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import com.github.lightcopy.{Catalog, IndexSpec, SimpleCatalog}
import com.github.lightcopy.index.simple.{SimpleIndex, SimpleSource}
import com.github.lightcopy.index.parquet.ParquetSource
import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

/** Test suite for [[SourceReader]] and interfaces */
class SourceReaderSuite extends UnitTestSuite {
  test("index source default fallback behaviour") {
    // should throw an unsupported exception
    val source = new IndexSource() {
      override def loadIndex(catalog: Catalog, metadata: Metadata): Index = ???
      override def createIndex(catalog: Catalog, spec: IndexSpec, cols: Seq[Column]): Index = ???
    }

    intercept[UnsupportedOperationException] {
      // parameters do not really matter, no validation is done
      source.fallback(null, null, null)
    }
  }

  test("check metadata file const") {
    SourceReader.METADATA_FILE should be ("_index_metadata")
  }

  test("check Parquet index source name") {
    SourceReader.PARQUET should be (ParquetSource.PARQUET_SOURCE_FORMAT)
  }

  test("check Simple index source name") {
    SourceReader.SIMPLE should be (SimpleSource.SIMPLE_SOURCE_FORMAT)
  }

  test("resolve parquet source") {
    SourceReader.resolveSource(SourceReader.PARQUET).isInstanceOf[ParquetSource] should be (true)
  }

  test("resolve simple source") {
    SourceReader.resolveSource(SourceReader.SIMPLE).isInstanceOf[SimpleSource] should be (true)
  }

  test("fail to resolve unknown source") {
    val err = intercept[UnsupportedOperationException] {
      SourceReader.resolveSource("invalid")
    }
    err.getMessage should be (
      "Source invalid is not supported, accepted sources are 'simple', 'parquet'")
  }

  test("generate metadata path") {
    // always append separator with metadata name
    SourceReader.metadataPath(new Path("/tmp")).toString should be ("/tmp/_index_metadata")
    SourceReader.metadataPath(new Path("/tmp/")).toString should be ("/tmp/_index_metadata")
    SourceReader.metadataPath(new Path("~")).toString should be ("~/_index_metadata")
    SourceReader.metadataPath(new Path("path")).toString should be ("path/_index_metadata")
  }

  test("read/write metadata 1") {
    withTempDir { dir =>
      val metadata = Metadata(
        "source",
        Some("path"),
        StructType(StructField("id", LongType) :: StructField("str", StringType) :: Nil),
        Map("key" -> "value"))
      SourceReader.writeMetadata(fs, dir, metadata)
      val res = SourceReader.readMetadata(fs, dir)
      res should be (metadata)
    }
  }

  test("read/write metadata 2") {
    withTempDir { dir =>
      val metadata = Metadata("source", None,
        StructType(StructField("a", LongType) :: Nil), Map.empty)
      SourceReader.writeMetadata(fs, dir, metadata)
      val res = SourceReader.readMetadata(fs, dir)
      res should be (metadata)
    }
  }

  test("with root directory for index") {
    withTempDir { dir =>
      val catalog = new SimpleCatalog() {
        override def getFreshIndexDirectory(): Path = dir
      }

      SourceReader.withRootDirectoryForIndex(catalog) { root =>
        val metadata = Metadata("source", None,
          StructType(StructField("a", LongType) :: Nil), Map.empty)
        SourceReader.writeMetadata(catalog.fs, root, metadata)
        null
      }

      fs.exists(dir) should be (true)
      fs.exists(dir.suffix(s"${Path.SEPARATOR}${SourceReader.METADATA_FILE}")) should be (true)
    }
  }

  test("fail with root directory for index") {
    // when failure occurs in closure directory should be removed and exception rethrown
    withTempDir { dir =>
      val catalog = new SimpleCatalog() {
        override def getFreshIndexDirectory(): Path = dir
      }

      val err = intercept[RuntimeException] {
        SourceReader.withRootDirectoryForIndex(catalog) { root =>
          sys.error("Test")
        }
      }

      err.getMessage should be ("Test")
      fs.exists(dir) should be (false)
    }
  }

  test("keep directory if file system fails to delete") {
    // when failure occurs in closure directory should be removed and exception rethrown
    withTempDir { dir =>
      val catalog = new SimpleCatalog() {
        override def fs: FileSystem = sys.error("No file system")
        override def getFreshIndexDirectory(): Path = dir
      }

      val err = intercept[RuntimeException] {
        SourceReader.withRootDirectoryForIndex(catalog) { root =>
          sys.error("Test")
        }
      }

      err.getMessage should be ("Test")
      fs.exists(dir) should be (true)
    }
  }

  test("load index") {
    withTempDir { dir =>
      val catalog = new SimpleCatalog()
      val status = catalog.fs.getFileStatus(dir)
      val index = new SimpleIndex(catalog)
      SourceReader.writeMetadata(catalog.fs, dir, index.getMetadata)

      val res = SourceReader.loadIndex(catalog, status)
      res.getMetadata should be (index.getMetadata)
      res.getIndexIdentifier should be (index.getIndexIdentifier)
    }
  }

  test("fail to load index (no metadata)") {
    withTempDir { dir =>
      val catalog = new SimpleCatalog()
      val status = catalog.fs.getFileStatus(dir)
      intercept[FileNotFoundException] {
        SourceReader.loadIndex(catalog, status)
      }
    }
  }

  test("create index") {
    withTempDir { dir =>
      val catalog = new SimpleCatalog() {
        override def getFreshIndexDirectory(): Path = dir
      }
      val spec = IndexSpec("simple", None, SaveMode.Append, Map.empty)
      val index = SourceReader.createIndex(catalog, spec, Seq.empty)
      val metadata = SourceReader.readMetadata(catalog.fs, dir)
      metadata should be (index.getMetadata)
      // verify that spec has set path
      spec.getConf(IndexSpec.INDEX_DIR) should be (dir.toString)
    }
  }

  test("source fallback") {
    val catalog = new SimpleCatalog()
    val spec = IndexSpec("simple", None, SaveMode.Append, Map.empty)
    val df = SourceReader.fallback(catalog, spec, lit(1) === lit(1))
    df should be (null)
  }
}
