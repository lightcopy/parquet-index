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

import java.io.OutputStream

import org.apache.commons.io.{IOUtils => CommonIOUtils}
import org.apache.hadoop.fs.{FileSystem, Path}

/** IO related utility methods */
object IOUtils {
  /** Read content as UTF-8 String from provided file */
  def readContent(fs: FileSystem, path: Path): String = {
    val in = fs.open(path)
    try {
      CommonIOUtils.toString(in, "UTF-8")
    } finally {
      in.close()
    }
  }

  /** Write content as UTF-8 String into provided file path, file is ovewritten on next attempt */
  def writeContent(fs: FileSystem, path: Path, content: String): Unit = {
    val out = fs.create(path, true)
    try {
      CommonIOUtils.write(content, out, "UTF-8")
    } finally {
      out.close()
    }
  }

  /** Write content into stream, file is overwritten on next attempt */
  def writeContentStream(fs: FileSystem, path: Path)(func: OutputStream => Unit): Unit = {
    val out = fs.create(path, true)
    try {
      func(out)
    } finally {
      out.close()
    }
  }
}
