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

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}

object Util {

  /** Read content as UTF-8 String from provided file */
  def readContent(fs: FileSystem, path: Path): String = {
    var in = fs.open(path)
    try {
      IOUtils.toString(in, "UTF-8")
    } finally {
      if (in != null) {
        in.close()
      }
    }
  }

  /** Write content as UTF-8 String into provided file path, file is ovewritten on next attempt */
  def writeContent(fs: FileSystem, path: Path, content: String): Unit = {
    var out = fs.create(path, true)
    try {
      IOUtils.write(content, out, "UTF-8")
    } finally {
      if (out != null) {
        out.close()
      }
    }
  }
}
