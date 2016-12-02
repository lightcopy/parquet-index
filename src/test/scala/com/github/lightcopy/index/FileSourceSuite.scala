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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, lit}

import com.github.lightcopy.{Catalog, IndexSpec, SimpleCatalog}
import com.github.lightcopy.testutil.{UnitTestSuite, SparkLocal}
import com.github.lightcopy.testutil.implicits._

// source that provides methods to validate parameters for creating index
class AssertFileSource extends FileSource {
  override def createFileIndex(
      catalog: Catalog,
      indexDir: FileStatus,
      root: FileStatus,
      paths: Array[FileStatus],
      colNames: Seq[String]): Index = {
    assertCatalog(catalog)
    assertIndexDir(indexDir)
    assertRoot(root)
    assertPaths(paths)
    assertColNames(colNames)
    null
  }

  override def loadFileIndex(catalog: Catalog, metadata: Metadata): Index = null

  // Assertions to validate API parameters
  def assertCatalog(catalog: Catalog): Unit = { }
  def assertIndexDir(indexDir: FileStatus): Unit = { }
  def assertRoot(root: FileStatus): Unit = { }
  def assertPaths(paths: Array[FileStatus]): Unit = { }
  def assertColNames(colNames: Seq[String]): Unit = { }
}

class FileSourceSuite extends UnitTestSuite with SparkLocal {
  private val catalog = new SimpleCatalog()
  private val source = new AssertFileSource()

  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("withColumnName - convert column name") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    source.withColumnName(lit(1)) should be ("1")
    source.withColumnName(lit("1")) should be ("1")
    source.withColumnName($"column") should be ("column")
    source.withColumnName($"user.path") should be ("user.path")
  }

  test("discoverPath - resolve directory") {
    withTempDir { dir =>
      val status = source.discoverPath(catalog, dir.toString)
      status.isDirectory should be (true)
      status.getPath.toString should be (s"file:$dir")
    }
  }

  test("discoverPath - resolve file path") {
    withTempDir { dir =>
      val file = dir.toString / "file"
      touch(file)
      val status = source.discoverPath(catalog, file)
      status.isDirectory should be (false)
      status.getPath.toString should be (s"file:$file")
    }
  }

  test("discoverPath - fail if path contains globstar") {
    withTempDir { dir =>
      val path = dir.toString / "*"
      val err = intercept[FileNotFoundException] {
        source.discoverPath(catalog, path)
      }
      err.getMessage.contains(s"Datasource path $path cannot be resolved") should be (true)
    }
  }

  test("discoverPath - fail if path contains regular expression") {
    withTempDir { dir =>
      val path = dir.toString / "a[0,1,2].txt"
      val err = intercept[FileNotFoundException] {
        source.discoverPath(catalog, path)
      }
      err.getMessage.contains(s"Datasource path $path cannot be resolved") should be (true)
    }
  }

  test("listStatuses - discover multiple files") {
    withTempDir { dir =>
      touch(dir.toString / "file1")
      touch(dir.toString / "file2")
      touch(dir.toString / "file3")
      touch(dir.toString / "file4")
      val root = source.discoverPath(catalog, dir.toString)
      val statuses = source.listStatuses(catalog, root)
      statuses.length should be (4)
      statuses.foreach { status =>
        status.isFile should be (true)
        status.getPath.getName.startsWith("file") should be (true)
      }
    }
  }

  test("listStatuses - return status for file") {
    withTempDir { dir =>
      val path = dir.toString / "file"
      touch(path)
      val statuses = source.listStatuses(catalog, source.discoverPath(catalog, path))
      statuses.length should be (1)
      statuses.head.getPath.toString should be (s"file:$path")
    }
  }

  test("pathFilter - check default path filter") {
    // default path filter should always return true, even for invalid paths
    val filter = source.pathFilter()
    filter.accept(null) should be (true)
    filter.accept(new Path("./")) should be (true)
    filter.accept(new Path("hdfs:/tmp/dir")) should be (true)
    filter.accept(new Path("hdfs:/tmp/dir/_SUCCESS")) should be (true)
  }

  test("createIndex - fail if index dir is not provided") {
    withTempDir { dir =>
      val spec = IndexSpec("source", None, SaveMode.Ignore, Map.empty)
      val err = intercept[RuntimeException] {
        source.createIndex(catalog, spec, Seq.empty)
      }
      err.getMessage.contains("Failed to look up value for key") should be (true)
    }
  }

  test("createIndex - fail if columns are empty") {
    withTempDir { dir =>
      val spec = IndexSpec("source", None, SaveMode.Ignore,
        Map(IndexSpec.INDEX_DIR -> dir.toString))
      val err = intercept[IllegalArgumentException] {
        source.createIndex(catalog, spec, Seq.empty)
      }
      err.getMessage.contains("Expected at least one column") should be (true)
    }
  }

  test("createIndex - fail if datasource path is not provided in spec") {
    withTempDir { dir =>
      val spec = IndexSpec("source", None, SaveMode.Ignore,
        Map(IndexSpec.INDEX_DIR -> dir.toString))
      val err = intercept[RuntimeException] {
        source.createIndex(catalog, spec, Seq(lit(1)))
      }
      err.getMessage.contains("does not contain path that is required") should be (true)
    }
  }

  test("createIndex - fail if datasource path does not contain files") {
    withTempDir { dir =>
      val spec = IndexSpec("source", Some(dir.toString), SaveMode.Ignore,
        Map(IndexSpec.INDEX_DIR -> dir.toString))
      val err = intercept[IllegalArgumentException] {
        source.createIndex(catalog, spec, Seq(lit(1)))
      }
      err.getMessage.contains("Expected at least one datasource file") should be (true)
    }
  }

  test("createIndex - fail if datasource path has nested directories with files") {
    // we only traverse first level of children, do not glob child directories
    withTempDir { dir =>
      touch(dir.toString / "dir1" / "file1")
      touch(dir.toString / "dir1" / "file2")
      touch(dir.toString / "dir2" / "file1")
      touch(dir.toString / "dir2" / "file2")
      val spec = IndexSpec("source", Some(dir.toString), SaveMode.Ignore,
        Map(IndexSpec.INDEX_DIR -> dir.toString))
      val err = intercept[IllegalArgumentException] {
        source.createIndex(catalog, spec, Seq(lit(1)))
      }
      err.getMessage.contains("Expected at least one datasource file") should be (true)
    }
  }

  test("createIndex - resolve all statuses for valid spec") {
    withTempDir { dir =>
      touch(dir.toString / "_SUCCESS")
      touch(dir.toString / "file1")
      touch(dir.toString / "file2")
      touch(dir.toString / "file3")
      touch(dir.toString / "file4")
      val columns = Seq(col("key1.path"), col("key2"), col("key3"))
      val spec = IndexSpec("source", Some(dir.toString), SaveMode.Ignore,
        Map(IndexSpec.INDEX_DIR -> dir.toString))

      val source = new AssertFileSource() {
        override def assertPaths(paths: Array[FileStatus]): Unit = {
          paths.length should be (5)
          paths.forall(_.isFile) should be (true)
          paths.exists(_.getPath.getName == "_SUCCESS") should be (true)
        }

        override def assertColNames(colNames: Seq[String]): Unit = {
          colNames should be (Seq("key1.path", "key2", "key3"))
        }
      }

      val index = source.createIndex(catalog, spec, columns)
      index should be (null)
    }
  }

  test("createIndex - return distinct column names for create index") {
    withTempDir { dir =>
      touch(dir.toString / "file1")
      val spec = IndexSpec("source", Some(dir.toString), SaveMode.Ignore,
        Map(IndexSpec.INDEX_DIR -> dir.toString))

      val source = new AssertFileSource() {
        override def assertColNames(colNames: Seq[String]): Unit = {
          // columns should be unique for file source
          colNames should be (colNames.toSet.toSeq)
        }
      }

      source.createIndex(catalog, spec, Seq(lit("abc"), lit("abc"), lit("bcd")))
      source.createIndex(catalog, spec, Seq(lit("abc"), lit("bcd")))
      source.createIndex(catalog, spec, Seq(lit("abc"), lit("ABC")))
    }
  }
}
