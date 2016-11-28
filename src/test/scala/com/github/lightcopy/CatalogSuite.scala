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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

/** Test suite for [[Catalog]], [[InternalCatalog]] and [[IndexSpec]] */
class CatalogSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("index spec - toString 1") {
    val spec = IndexSpec("test-source", Some("path"), SaveMode.Append, Map.empty)
    spec.toString should be (
      "IndexSpec(source=test-source, path=Some(path), mode=Append, options=Map())")
  }

  test("index spec - toString 2") {
    val spec = IndexSpec("test-source", None, SaveMode.Ignore, Map.empty)
    spec.toString should be (
      "IndexSpec(source=test-source, path=None, mode=Ignore, options=Map())")
  }

  test("index spec - toString 3") {
    val spec = IndexSpec("test-source", None, SaveMode.Ignore, Map("1" -> "2", "3" -> "4"))
    spec.toString should be (
      "IndexSpec(source=test-source, path=None, mode=Ignore, options=Map(1 -> 2, 3 -> 4))")
  }

  test("index spec - setConf") {
    val spec = IndexSpec("test-source", None, SaveMode.Ignore, Map("key" -> "value"))
    // set new key and overwrite existing key
    spec.setConf("key", "value1")
    spec.setConf("key2", "value2")
    // check that options are unchanged
    spec.options should be (Map("key" -> "value"))
    spec.getConf("key") should be ("value1")
    spec.getConf("key2") should be ("value2")
  }

  test("index spec - getConf for existing key") {
    val spec = IndexSpec("test-source", None, SaveMode.Ignore, Map.empty)
    spec.setConf("key", "value")
    spec.getConf("key") should be ("value")
  }

  test("index spec - getConf for non-existing key") {
    val spec = IndexSpec("test-source", None, SaveMode.Ignore, Map.empty)
    intercept[RuntimeException] {
      spec.getConf("key")
    }
  }

  test("index spec - getConf for non-existing key and existing default") {
    val spec = IndexSpec("test-source", None, SaveMode.Ignore, Map.empty)
    spec.getConf("key", "default") should be ("default")
  }

  test("internal catalog metastore option") {
    InternalCatalog.METASTORE_OPTION should be ("spark.sql.index.metastore")
  }

  test("internal catalog metastore directory name") {
    InternalCatalog.DEFAULT_METASTORE_DIR should be ("index_metastore")
  }

  test("internal catalog metastore default permission") {
    InternalCatalog.METASTORE_PERMISSION.toString should be ("rwxrw-rw-")
  }

  test("get metastore path without setting") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val catalog = new InternalCatalog(spark.sqlContext)
      // unset catalog option to test try-option block
      spark.conf.unset(InternalCatalog.METASTORE_OPTION)
      val res = catalog.getMetastorePath(catalog.sqlContext)
      res should be (None)
    }
  }

  test("get metastore path with setting") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val catalog = new InternalCatalog(spark.sqlContext)
      val res = catalog.getMetastorePath(catalog.sqlContext)
      res should be (Some(dir.toString))
    }
  }

  test("resolve metastore for non-existent directory") {
    withTempDir { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION,
        dir.suffix(Path.SEPARATOR + "test_metastore").toString)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.metastorePath.endsWith("test_metastore") should be (true)
      val status = fs.getFileStatus(new Path(catalog.metastorePath))
      status.isDirectory should be (true)
      status.getPermission should be (InternalCatalog.METASTORE_PERMISSION)
    }
  }

  test("use existing directory") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.metastorePath should be ("file:" + dir.toString)
      val status = fs.getFileStatus(new Path(catalog.metastorePath))
      status.isDirectory should be (true)
      status.getPermission should be (InternalCatalog.METASTORE_PERMISSION)
    }
  }

  test("fail if metastore is not a directory ") {
    withTempDir { dir =>
      val path = dir.suffix(Path.SEPARATOR + "file")
      fs.createNewFile(path)
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, path.toString)
      // metastore is resolved when catalog is initialized
      val err = intercept[IllegalStateException] {
        new InternalCatalog(spark.sqlContext)
      }
      err.getMessage.contains("Expected directory for metastore") should be (true)
    }
  }

  test("fail if metastore has insufficient permissions") {
    withTempDir(new FsPermission("444")) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      // metastore is resolved when catalog is initialized
      val err = intercept[IllegalStateException] {
        new InternalCatalog(spark.sqlContext)
      }
      err.getMessage.contains("Expected directory with rwxrw-rw-") should be (true)
      err.getMessage.contains("r--r--r--") should be (true)
    }
  }

  test("use directory with richer permissions") {
    withTempDir(new FsPermission("777")) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.metastorePath should be ("file:" + dir.toString)
    }
  }

  test("get fresh index directory") {
    withTempDir(new FsPermission("777")) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      val path = catalog.getFreshIndexDirectory
      path.getParent.toString should be (catalog.metastorePath)
    }
  }

  test("fail on directory collision") {
    withTempDir { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION,
        dir.suffix(Path.SEPARATOR + "test_metastore").toString)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext) {
        override def getRandomName(): String = ""
      }
      val err = intercept[IllegalStateException] {
        catalog.getFreshIndexDirectory
      }
      err.getMessage.contains("Fresh directory collision") should be (true)
    }
  }

  test("fail if fresh directory cannot be created") {
    withTempDir { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION,
        dir.suffix(Path.SEPARATOR + "test_metastore").toString)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      val err = intercept[IllegalStateException] {
        fs.setPermission(dir, new FsPermission("444"))
        catalog.getFreshIndexDirectory
      }
      err.getMessage.contains("Failed to create new directory") should be (true)
    }
  }

  test("list indexes for empty metastore") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.listIndexes should be (Seq.empty)
    }
  }

  test("create/list simple index in metastore") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val spec = IndexSpec("simple", None, SaveMode.ErrorIfExists, Map.empty)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.createIndex(spec, Seq.empty)

      catalog.listIndexes.length should be (1)
      val index = catalog.getIndex(spec)
      index.isDefined should be (true)
      index.get.getIndexIdentifier should be ("simple")
    }
  }

  test("create index with error save mode") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val spec = IndexSpec("simple", None, SaveMode.ErrorIfExists, Map.empty)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.createIndex(spec, Seq.empty)
      val err = intercept[IllegalStateException] {
        catalog.createIndex(spec, Seq.empty)
      }
      err.getMessage.contains("Index already exists for spec")
    }
  }

  test("create index with overwrite save mode") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val spec = IndexSpec("simple", None, SaveMode.Overwrite, Map.empty)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.createIndex(spec, Seq.empty)
      catalog.createIndex(spec, Seq.empty)
      catalog.listIndexes.length should be (1)
    }
  }

  test("create index with append save mode") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val spec = IndexSpec("simple", None, SaveMode.Append, Map.empty)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.createIndex(spec, Seq.empty)
      catalog.createIndex(spec, Seq.empty)
      catalog.listIndexes.length should be (1)
    }
  }

  test("create index with ignore save mode") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val spec = IndexSpec("simple", None, SaveMode.Ignore, Map.empty)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.createIndex(spec, Seq.empty)
      catalog.createIndex(spec, Seq.empty)
      catalog.listIndexes.length should be (1)
    }
  }

  test("drop non-existing index") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val spec = IndexSpec("simple", None, SaveMode.Ignore, Map.empty)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      // deletion should be no-op
      catalog.dropIndex(spec)
    }
  }

  test("drop existing index") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val spec = IndexSpec("simple", None, SaveMode.Ignore, Map.empty)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.createIndex(spec, Seq.empty)
      catalog.listIndexes.length should be (1)

      catalog.dropIndex(spec)
      catalog.listIndexes.length should be (0)
    }
  }

  test("query non-existing index, use source fallback") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val spec = IndexSpec("simple", None, SaveMode.Ignore, Map.empty)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      val df = catalog.queryIndex(spec, lit(1) === lit(2))
      // SimpleSource returns null DataFrame for fallback
      df should be (null)
    }
  }

  test("query existing index") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val spec = IndexSpec("simple", None, SaveMode.Ignore, Map.empty)
      // metastore is resolved when catalog is initialized
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.createIndex(spec, Seq.empty)
      val df = catalog.queryIndex(spec, lit(1) === lit(2))
      df should be (null)
    }
  }

  test("internal catalog - toString") {
    withTempDir(InternalCatalog.METASTORE_PERMISSION) { dir =>
      spark.sqlContext.setConf(InternalCatalog.METASTORE_OPTION, dir.toString)
      val catalog = new InternalCatalog(spark.sqlContext)
      catalog.toString should be (s"InternalCatalog[metastore=file:$dir]")
    }
  }
}
