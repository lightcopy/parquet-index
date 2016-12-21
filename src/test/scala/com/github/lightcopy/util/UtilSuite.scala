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

import java.io.{ObjectInputStream, ObjectOutputStream}

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

  test("read content into stream") {
    withTempDir { dir =>
      val path = dir.toString / "test"
      touch(path)
      val bytes = new Array[Byte](128)
      IOUtils.readContentStream(fs, new HadoopPath(path)) { in =>
        in.read(bytes)
      }
      bytes.sum should be (0)
    }
  }

  test("write content into stream") {
    withTempDir { dir =>
      val path = dir.toString / "test"
      val bytes = "test-content".getBytes()
      IOUtils.writeContentStream(fs, new HadoopPath(path)) { out =>
        out.write(bytes)
      }
      IOUtils.readContent(fs, new HadoopPath(path)) should be ("test-content")
    }
  }

  test("Hadoop configuration - read/write object") {
    val conf = new Configuration(false)
    conf.set("test.key", "test.value")

    withTempDir { dir =>
      val path = dir.toString / "obj.tmp"
      IOUtils.writeContentStream(fs, new HadoopPath(path)) { out =>
        new ObjectOutputStream(out).writeObject(new SerializableConfiguration(conf))
      }

      // try reading object from the file and check for consistency
      IOUtils.readContentStream(fs, new HadoopPath(path)) { in =>
        val deserial = new ObjectInputStream(in).readObject().
          asInstanceOf[SerializableConfiguration]
        deserial.value.get("test.key") should be ("test.value")
      }
    }
  }

  test("SerializableFileStatus - from file status conversion") {
    withTempDir { dir =>
      val status = fs.getFileStatus(dir)
      val serde = SerializableFileStatus.fromFileStatus(status)
      serde.path should be (status.getPath.toString)
      serde.length should be (status.getLen)
      serde.isDir should be (status.isDirectory)
      serde.blockReplication should be (status.getReplication)
      serde.blockSize should be (status.getBlockSize)
      serde.modificationTime should be (status.getModificationTime)
      serde.accessTime should be (status.getAccessTime)
    }
  }

  test("SerializableFileStatus - from/to file status conversion") {
    withTempDir { dir =>
      val status = fs.getFileStatus(dir)
      val serde = SerializableFileStatus.fromFileStatus(status)
      val result = SerializableFileStatus.toFileStatus(serde)
      result should be (status)
    }
  }
}
