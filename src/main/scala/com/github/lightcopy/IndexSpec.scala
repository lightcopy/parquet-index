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

import scala.collection.Map
import scala.collection.mutable.HashMap

import org.apache.spark.sql.SaveMode

/**
 * Partial index specification to access index in metastore.
 * @param source format of the index
 * @param path optional path for datasource table
 * @param mode save mode (only applicable for creating index)
 * @param options specific index options, e.g. index directory
 */
case class IndexSpec(
    source: String,
    path: Option[String],
    mode: SaveMode,
    options: Map[String, String]) {
  // set of changeable internal options, do not use `options` set in constructor
  private val internalOptions = new HashMap[String, String]()
  internalOptions ++= options

  /** Set configuration for index spec */
  def setConf(key: String, value: String): Unit = {
    internalOptions.put(key, value)
  }

  /** Get configuration for index spec */
  def getConf(key: String): String = {
    internalOptions.getOrElse(key, sys.error(s"Failed to look up value for key $key in spec"))
  }

  /** Get configuration for index spec, and use default if not found */
  def getConf(key: String, default: String): String = {
    internalOptions.getOrElse(key, default)
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}(source=$source, path=$path, mode=$mode, " +
      s"options=$internalOptions)"
  }
}

/** Container for spec related configuration */
object IndexSpec {
  val INDEX_DIR = "indexDir"
}
