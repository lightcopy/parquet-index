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

package org.apache.spark.sql.internal

import org.apache.spark.internal.config._

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class IndexConfSuite extends UnitTestSuite with SparkLocal {
  import IndexConf._

  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  before {
    IndexConf.reset()
  }

  test("fail when registering duplicate key") {
    val entry = IndexConfigBuilder("test.key").stringConf.createWithDefault("test.value")

    // fail to register the same entry
    var err = intercept[IllegalArgumentException] {
      IndexConf.register(entry)
    }
    assert(err.getMessage.contains("Duplicate ConfigEntry"))

    // fail to register new entry with the same key
    err = intercept[IllegalArgumentException] {
      IndexConf.register(
        IndexConfigBuilder("test.key").stringConf.createWithDefault("test.value"))
    }
    assert(err.getMessage.contains("Duplicate ConfigEntry"))
  }

  test("create new conf") {
    val conf = IndexConf.newConf(spark)
    assert(conf.settings.size() >= 0)
  }

  test("create new conf with registered entries") {
    val entry = IndexConfigBuilder("test.key").stringConf.createWithDefault("test.value")
    val conf = IndexConf.newConf(spark)
    conf.getConf(entry) should be ("test.value")
  }

  test("set null key or value in conf") {
    val conf = new IndexConf()
    var err = intercept[IllegalArgumentException] {
      conf.setConfString(null, "")
    }
    assert(err.getMessage.contains("key cannot be null"))

    err = intercept[IllegalArgumentException] {
      conf.setConfString("", null)
    }
    assert(err.getMessage.contains("value cannot be null"))
  }

  test("set null entry") {
    val conf = new IndexConf()
    var err = intercept[IllegalArgumentException] {
      conf.setConf(null, "test")
    }
    assert(err.getMessage.contains("entry cannot be null"))

    val entry = IndexConfigBuilder("test.key").stringConf.createWithDefault("test.value")
    err = intercept[IllegalArgumentException] {
      conf.setConf(entry, null)
    }
    assert(err.getMessage.contains("value cannot be null"))
  }

  test("set key for existing entry") {
    val entry = IndexConfigBuilder("test.key").stringConf.createWithDefault("test.value")
    val conf = new IndexConf()
    conf.setConf(entry, "test.value1")
    conf.setConfString("test.key", "test.value2")
    conf.getAllConfs should be (Map("test.key" -> "test.value2"))
  }

  test("get existing key") {
    val entry = IndexConfigBuilder("test.key").stringConf.createWithDefault("test.value")
    val conf = new IndexConf()
    conf.getConf(entry) should be ("test.value")
  }

  test("fail to get non-existing key") {
    val conf = new IndexConf()
    val entry = ConfigBuilder("test.key").stringConf.createWithDefault("test.value")
    intercept[IllegalArgumentException] {
      conf.getConf(entry)
    }
  }

  test("fail to get unregistered key") {
    val entry = IndexConfigBuilder("test.key").stringConf.createOptional
    val conf = new IndexConf()
    intercept[NoSuchElementException] {
      conf.getConf(entry)
    }
  }

  test("unset key") {
    val entry = IndexConfigBuilder("test.key").stringConf.createWithDefault("test.value")
    val conf = new IndexConf()
    conf.setConf(entry, "test.value1")
    conf.unsetConf(entry.key)
    conf.getAllConfs should be (Map.empty)
  }

  test("unset entry") {
    val entry = IndexConfigBuilder("test.key").stringConf.createWithDefault("test.value")
    val conf = new IndexConf()
    conf.setConf(entry, "test.value1")
    conf.unsetConf(entry)
    conf.getAllConfs should be (Map.empty)
  }

  test("clear settings") {
    val conf = new IndexConf()
    conf.setConfString("key1", "value1")
    conf.setConfString("key2", "value2")
    conf.getAllConfs.nonEmpty should be (true)
    conf.clear()
    conf.getAllConfs.nonEmpty should be (false)
  }
}
