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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

import com.github.lightcopy.{Catalog, IndexSpec, SimpleCatalog}
import com.github.lightcopy.testutil.{UnitTestSuite, SparkLocal}
import com.github.lightcopy.testutil.implicits._

class FileSourceSuite extends UnitTestSuite with SparkLocal {
  private val catalog = new SimpleCatalog()
  private val source = new FileSource() {
    override def createFileIndex(
        catalog: Catalog,
        indexDir: FileStatus,
        root: FileStatus,
        paths: Array[FileStatus],
        colNames: Seq[String]): Index = null

    override def loadFileIndex(
        catalog: Catalog,
        metadata: Metadata): Index = null
  }


  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("convert column name") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    source.withColumnName(lit(1)) should be ("1")
    source.withColumnName($"column") should be ("column")
  }

  test("discover path") {
    withTempDir { dir =>
      val status = source.discoverPath(catalog, dir.toString)
      status.isDirectory should be (true)
      status.getPath.toString should be (s"file:$dir")
    }
  }

  test("fail with meaningful exception when discovering path") {
    withTempDir { dir =>
      val path = dir.suffix("*").toString
      val err = intercept[FileNotFoundException] {
        source.discoverPath(catalog, path)
      }
      err.getMessage.contains(s"Datasource path $path cannot be resolved") should be (true)
    }
  }

  test("discover multiple paths") {
    withTempDir { dir =>
      mkdirs(dir.suffix(s"${Path.SEPARATOR}subdir1").toString)
      mkdirs(dir.suffix(s"${Path.SEPARATOR}subdir2").toString)
      create(dir.suffix(s"${Path.SEPARATOR}file1").toString).close()
      create(dir.suffix(s"${Path.SEPARATOR}file2").toString).close()
      val root = source.discoverPath(catalog, dir.toString)
      val statuses = source.listStatuses(catalog, root)
      statuses.length should be (2) // found file1 and file2
      statuses.foreach { status =>
        status.isFile should be (true)
        status.getPath.getName.startsWith("file") should be (true)
      }
    }
  }

  test("discover single path") {
    withTempDir { dir =>
      val path = dir.suffix(s"${Path.SEPARATOR}file").toString
      create(path).close()
      val root = source.discoverPath(catalog, path)
      val statuses = source.listStatuses(catalog, root)
      statuses.length should be (1)
      statuses.head.getPath.toString should be (s"file:$path")
    }
  }

  test("fail if index dir is not provided") {
    withTempDir { dir =>
      val spec = IndexSpec("source", None, SaveMode.Ignore, Map.empty)
      val err = intercept[RuntimeException] {
        source.createIndex(catalog, spec, Seq.empty)
      }
      err.getMessage.contains("Failed to look up value for key") should be (true)
    }
  }

  test("fail if columns are empty") {
    withTempDir { dir =>
      val spec = IndexSpec("source", None, SaveMode.Ignore,
        Map(IndexSpec.INDEX_DIR -> dir.toString))
      val err = intercept[IllegalArgumentException] {
        source.createIndex(catalog, spec, Seq.empty)
      }
      err.getMessage.contains("Expected at least one column") should be (true)
    }
  }

  test("fail if datasource path does not contain files") {
    withTempDir { dir =>
      val spec = IndexSpec("source", Some(dir.toString), SaveMode.Ignore,
        Map(IndexSpec.INDEX_DIR -> dir.toString))
      val err = intercept[IllegalArgumentException] {
        source.createIndex(catalog, spec, Seq(lit(1)))
      }
      err.getMessage.contains("Expected at least one datasource file") should be (true)
    }
  }

  test("fail if datasource path is not provided in spec") {
    withTempDir { dir =>
      create(dir.suffix(s"${Path.SEPARATOR}file").toString).close()
      val spec = IndexSpec("source", None, SaveMode.Ignore,
        Map(IndexSpec.INDEX_DIR -> dir.toString))
      val err = intercept[RuntimeException] {
        source.createIndex(catalog, spec, Seq(lit(1)))
      }
      err.getMessage.contains("does not contain path that is required") should be (true)
    }
  }

  test("return dummy index when checks passed") {
    withTempDir { dir =>
      create(dir.suffix(s"${Path.SEPARATOR}file").toString).close()
      val spec = IndexSpec("source", Some(dir.toString), SaveMode.Ignore,
        Map(IndexSpec.INDEX_DIR -> dir.toString))
      val index = source.createIndex(catalog, spec, Seq(lit(1)))
      index should be (null)
    }
  }

  test("check default path filter") {
    // default path filter should always return true, even for invalid paths
    val filter = source.pathFilter()
    filter.accept(null) should be (true)
    filter.accept(new Path("./")) should be (true)
    filter.accept(new Path("hdfs:/tmp/dir")) should be (true)
    filter.accept(new Path("hdfs:/tmp/dir/_SUCCESS")) should be (true)
  }
}
