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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.Column

import com.github.lightcopy.{Catalog, IndexSpec, SimpleCatalog}
import com.github.lightcopy.index.parquet.ParquetSource
import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

/** Test suite for [[Source]] and interfaces */
class SourceSuite extends UnitTestSuite {
  test("index source default fallback behaviour") {
    // should throw an unsupported exception
    val source = new IndexSource() {
      override def loadIndex(catalog: Catalog, metadata: Metadata): Index = ???
      override def createIndex(
        catalog: Catalog, spec: IndexSpec, dir: String, columns: Seq[Column]): Index = ???
    }

    intercept[UnsupportedOperationException] {
      // parameters do not really matter, no validation is done
      source.fallback(null, null, null)
    }
  }

  test("check metadata file const") {
    Source.METADATA_FILE should be ("_index_metadata")
  }

  test("check Parquet index source name") {
    Source.PARQUET should be ("parquet")
  }

  test("resolve parquet source") {
    Source.resolveSource(Source.PARQUET).isInstanceOf[ParquetSource] should be (true)
  }

  test("fail to resolve unknown source") {
    val err = intercept[UnsupportedOperationException] {
      Source.resolveSource("invalid")
    }
    err.getMessage should be ("Source invalid is not supported, accepted sources are 'parquet'")
  }

  test("generate metadata path") {
    // always append separator with metadata name
    Source.metadataPath(new Path("/tmp")).toString should be ("/tmp/_index_metadata")
    Source.metadataPath(new Path("/tmp/")).toString should be ("/tmp/_index_metadata")
    Source.metadataPath(new Path("~")).toString should be ("~/_index_metadata")
    Source.metadataPath(new Path("path")).toString should be ("path/_index_metadata")
  }

  test("read/write metadata 1") {
    withTempDir { dir =>
      val fs = dir.getFileSystem(new Configuration(false))
      val metadata = Metadata(
        "source",
        Some("path"),
        Seq(ColumnSpec("id", "long"), ColumnSpec("str", "string")),
        Map("key" -> "value"))
      Source.writeMetadata(fs, dir, metadata)
      val res = Source.readMetadata(fs, dir)
      res should be (metadata)
    }
  }

  test("read/write metadata 2") {
    withTempDir { dir =>
      val fs = dir.getFileSystem(new Configuration(false))
      val metadata = Metadata("source", None, Seq.empty, Map.empty)
      Source.writeMetadata(fs, dir, metadata)
      val res = Source.readMetadata(fs, dir)
      res should be (metadata)
    }
  }

  test("with root directory for index") {
    withTempDir { dir =>
      val catalog = new SimpleCatalog() {
        override def getFreshIndexDirectory(): Path = dir
      }

      Source.withRootDirectoryForIndex(catalog) { root =>
        val metadata = Metadata("source", None, Seq.empty, Map.empty)
        Source.writeMetadata(catalog.fs, root, metadata)
        null
      }

      val fs = dir.getFileSystem(new Configuration(false))
      fs.exists(dir) should be (true)
      fs.exists(dir.suffix(s"${Path.SEPARATOR}${Source.METADATA_FILE}")) should be (true)
    }
  }

  test("fail with root directory for index") {
    // when failure occurs in closure directory should be removed and exception rethrown
    withTempDir { dir =>
      val catalog = new SimpleCatalog() {
        override def getFreshIndexDirectory(): Path = dir
      }

      val err = intercept[RuntimeException] {
        Source.withRootDirectoryForIndex(catalog) { root =>
          sys.error("Test")
        }
      }

      err.getMessage should be ("Test")
      val fs = dir.getFileSystem(new Configuration(false))
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
        Source.withRootDirectoryForIndex(catalog) { root =>
          sys.error("Test")
        }
      }

      err.getMessage should be ("Test")
      val fs = dir.getFileSystem(new Configuration(false))
      fs.exists(dir) should be (true)
    }
  }
}
