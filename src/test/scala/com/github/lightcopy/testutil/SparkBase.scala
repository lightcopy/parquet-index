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

import scala.util.Try

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/** General Spark base */
private[testutil] trait SparkBase {
  @transient private[testutil] var _spark: SparkSession = null

  def createSparkSession(): SparkSession

  /** Start (or create) Spark session */
  def startSparkSession(): Unit = {
    stopSparkSession()
    setLoggingLevel(Level.ERROR)
    _spark = createSparkSession()
  }

  /** Stop Spark session */
  def stopSparkSession(): Unit = {
    if (_spark != null) {
      _spark.stop()
    }
    _spark = null
  }

  /**
   * Set logging level globally for all.
   * Supported log levels:
   *      Level.OFF
   *      Level.ERROR
   *      Level.WARN
   *      Level.INFO
   * @param level logging level
   */
  def setLoggingLevel(level: Level) {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
    Logger.getRootLogger().setLevel(level)
  }

  /** Returns Spark session */
  def spark: SparkSession = _spark

  /** Allow tests to set custom SQL configuration for duration of the closure */
  def withSQLConf(pairs: (String, String)*)(func: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(spark.conf.get(key)).toOption)
    (keys, values).zipped.foreach(spark.conf.set)
    try func finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  /** Allow tests to set custom SQL configuration for duration of closure using map of options */
  def withSQLConf(options: Map[String, String])(func: => Unit): Unit = {
    withSQLConf(options.toSeq: _*)(func)
  }
}
