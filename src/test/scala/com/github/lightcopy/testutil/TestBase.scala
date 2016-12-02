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

package com.github.lightcopy.testutil

import java.io.{InputStream, OutputStream}
import java.util.UUID

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.sql.{DataFrame, Row}

import com.github.lightcopy.testutil.implicits._

trait TestBase {
  val RESOLVER = "path-resolver"

  var path: String = ""

  // local file system for tests
  val fs = FileSystem.get(new HadoopConf(false))

  /** returns raw path of the folder where it finds resolver */
  private def getRawPath(): String = {
    if (path.isEmpty) {
      path = getClass().getResource("/" + RESOLVER).getPath()
    }
    path
  }

  /** base directory of the project */
  final protected def baseDirectory(): String = {
    val original = getRawPath().split("/")
    require(original.length > 4, s"Path length is too short (<= 4): ${original.length}")
    val base = original.dropRight(4)
    var dir = ""
    for (suffix <- base) {
      if (suffix.nonEmpty) {
        dir = dir / suffix
      }
    }
    dir
  }

  /** main directory of the project (./src/main) */
  final protected def mainDirectory(): String = {
    baseDirectory() / "src" / "main"
  }

  /** test directory of the project (./src/test) */
  final protected def testDirectory(): String = {
    baseDirectory() / "src" / "test"
  }

  /** target directory of the project (./target) */
  final protected def targetDirectory(): String = {
    baseDirectory() / "target"
  }

  final protected def mkdirs(path: String): Boolean = {
    val p = new HadoopPath(path)
    fs.mkdirs(p)
  }

  /** create empty file, similar to "touch" shell command, but creates intermediate directories */
  final protected def touch(path: String): Boolean = {
    val p = new HadoopPath(path)
    fs.mkdirs(p.getParent)
    fs.createNewFile(p)
  }

  /** delete directory / file with path. Recursive must be true for directory */
  final protected def rm(path: String, recursive: Boolean): Boolean = {
    val p = new HadoopPath(path)
    fs.delete(p, recursive)
  }

  /** open file for a path */
  final protected def open(path: String): InputStream = {
    val p = new HadoopPath(path)
    fs.open(p)
  }

  /** create file with a path and return output stream */
  final protected def create(path: String): OutputStream = {
    val p = new HadoopPath(path)
    fs.create(p)
  }

  /** compare two DataFrame objects */
  final protected def checkAnswer(df: DataFrame, expected: DataFrame): Unit = {
    val got = df.collect().map(_.toString()).sortWith(_ < _)
    val exp = expected.collect().map(_.toString()).sortWith(_ < _)
    assert(got.sameElements(exp), s"Failed to compare DataFrame ${got.mkString("[", ", ", "]")} " +
      s"with expected input ${exp.mkString("[", ", ", "]")}")
  }

  final protected def checkAnswer(df: DataFrame, expected: Seq[Row]): Unit = {
    val sc = df.sqlContext.sparkContext
    checkAnswer(df, df.sqlContext.createDataFrame(sc.parallelize(expected), df.schema))
  }

  /** Create temporary directory on local file system */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "lightcopy"): HadoopPath = {
    val dir = new HadoopPath(root / namePrefix / UUID.randomUUID().toString)
    fs.mkdirs(dir)
    dir
  }

  /** Execute block of code with temporary hadoop path and path permission */
  private def withTempHadoopPath(
      path: HadoopPath, permission: Option[FsPermission])(func: HadoopPath => Unit): Unit = {
    try {
      if (permission.isDefined) {
        fs.setPermission(path, permission.get)
      }
      func(path)
    } finally {
      fs.delete(path, true)
    }
  }


  /** Execute code block with created temporary directory with provided permission */
  def withTempDir(permission: FsPermission)(func: HadoopPath => Unit): Unit = {
    withTempHadoopPath(createTempDir(), Some(permission))(func)
  }

  /** Execute code block with created temporary directory */
  def withTempDir(func: HadoopPath => Unit): Unit = {
    withTempHadoopPath(createTempDir(), None)(func)
  }
}
