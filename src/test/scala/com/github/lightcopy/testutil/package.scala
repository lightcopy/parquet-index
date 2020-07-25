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

import java.io.File

import org.apache.hadoop.fs.{Path => HadoopPath}

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

package object implicits {
  implicit class PathStringBuilder(path: String) {
    def /(suffix: String): String = path + File.separator + suffix

    def `:`(append: String): String = path + File.pathSeparator + append
  }

  implicit class PathBuilder(path: HadoopPath) {
    def /(suffix: String): HadoopPath = path.suffix(s"${HadoopPath.SEPARATOR}$suffix")
  }
}

/** abstract general testing class */
abstract class UnitTestSuite extends AnyFunSuite with Matchers
  with TestBase with ParquetLogging with BeforeAndAfterAll with BeforeAndAfter
