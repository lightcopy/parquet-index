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

package com.github.lightcopy.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

class UtilSuite extends UnitTestSuite {
  test("read content from empty file") {
    withTempDir { dir =>
      val path = dir.toString / "test"
      touch(path)
      val content = IOUtils.readContent(fs, new HadoopPath(path))
      content.isEmpty should be (true)
    }
  }

  test("read content from non-empty file") {
    withTempDir { dir =>
      val path = dir.toString / "test"
      val out = create(path)
      out.write("test-content#".getBytes)
      out.close()
      IOUtils.readContent(fs, new HadoopPath(path)) should be ("test-content#")
    }
  }

  test("write content into file") {
    withTempDir { dir =>
      val path = new HadoopPath(dir.toString / "test")
      IOUtils.writeContent(fs, path, "test-content")
      IOUtils.readContent(fs, path) should be ("test-content")
    }
  }
}
