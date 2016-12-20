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

import java.util.{NoSuchElementException, Properties}

import scala.collection.JavaConverters._
import scala.collection.immutable

import org.apache.spark.internal.config._

/**
 * All available configuration for index functionality, similar to `SQLConf`.
 * Used as part of the metastore.
 */
private[spark] object IndexConf {
  private val confEntries = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, ConfigEntry[_]]())

  def register(entry: ConfigEntry[_]): Unit = confEntries.synchronized {
    require(!confEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    confEntries.put(entry.key, entry)
  }

  object IndexConfigBuilder {
    def apply(key: String): ConfigBuilder = {
      new ConfigBuilder(key).onCreate(register)
    }
  }

  // metastore location (root directory in case of file system)
  // will be created, if it does not exist
  val METASTORE_LOCATION = IndexConfigBuilder("spark.sql.index.metastore").
    doc("Metastore location or root directory to store index information, will be created " +
      "if path does not exist").
    stringConf.
    createWithDefault("")

  val CREATE_INDEX_WHEN_NOT_EXISTS = IndexConfigBuilder("spark.sql.index.createNotExists").
    doc("When set to true creates index during table read, if index does not exist in metastore").
    booleanConf.
    createWithDefault(false)

  // option to enable/disable bloom filters for Parquet index
  val PARQUET_BLOOM_FILTER_ENABLED = IndexConfigBuilder("spark.sql.index.parquet.bloom.enabled").
    doc("Whet set to true writes bloom filters for indexed columns when creating table index").
    booleanConf.
    createWithDefault(false)
}

private[spark] class IndexConf extends Serializable {
  import IndexConf._

  /** Only low degree of contention is expected for conf, thus NOT using ConcurrentHashMap. */
  @transient private val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  // == Index configuration ==
  def metastoreLocation: String = getConf(METASTORE_LOCATION)

  def createIndexWhenNotExists: Boolean = getConf(CREATE_INDEX_WHEN_NOT_EXISTS)

  def parquetBloomFilterEnabled: Boolean = getConf(PARQUET_BLOOM_FILTER_ENABLED)

  /** Set the given configuration property using a `string` value. */
  def setConfString(key: String, value: String): Unit = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    val entry = confEntries.get(key)
    if (entry != null) {
      entry.valueConverter(value)
    }
    setConfWithCheck(key, value)
  }

  /** Set the given index configuration property. */
  def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    require(confEntries.get(entry.key) == entry, s"$entry is not registered")
    setConfWithCheck(entry.key, entry.stringConverter(value))
  }

  /**
   * Return the value of index configuration entry property for the given key. If the key is not
   * set yet, return `defaultValue` in `ConfigEntry`.
   */
  def getConf[T](entry: ConfigEntry[T]): T = {
    require(confEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).orElse(entry.defaultValue).
      getOrElse(throw new NoSuchElementException(entry.key))
  }

  /**
   * Return all the configuration properties that have been set (i.e. not the default).
   * This creates a new copy of the config properties in the form of a Map.
   */
  def getAllConfs: immutable.Map[String, String] =
    settings.synchronized { settings.asScala.toMap }

  /**
   * Update value in configuration.
   */
  private def setConfWithCheck(key: String, value: String): Unit = {
    settings.put(key, value)
  }

  /**
   * Unset configuration for a given key.
   */
  def unsetConf(key: String): Unit = {
    settings.remove(key)
  }

  /**
   * Unset configuration for a given key of configuration entry.
   */
  def unsetConf(entry: ConfigEntry[_]): Unit = {
    settings.remove(entry.key)
  }

  /**
   * Clear configuration.
   */
  def clear(): Unit = {
    settings.clear()
  }
}
