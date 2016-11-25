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

import org.scalatest._

package object implicits {
  implicit class PathBuilder(path: String) {
    def /(suffix: String): String = path + File.separator + suffix

    def `:`(append: String): String = path + File.pathSeparator + append
  }
}

/** abstract general testing class */
abstract class UnitTestSuite extends FunSuite with Matchers with OptionValues with Inside
  with Inspectors with TestBase with BeforeAndAfterAll with BeforeAndAfter
