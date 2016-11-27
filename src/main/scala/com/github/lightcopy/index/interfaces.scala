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

package com.github.lightcopy.index

import org.apache.spark.sql.{Column, DataFrame}

import com.github.lightcopy.{Catalog, IndexSpec}

/**
 * Base class representing index. [[Index]] always has a root directory and name which is a name of
 * folder that is allocated for index. Metadata should always be stored on disk, regardless of
 * underlying implementation for storing index data.
 *
 * Index should load and verify metadata (and potentially data) when creating an instance, but this
 * can be partially done when calling `search(...)` method.
 */
abstract class Index {
  /** Get name of the index, e.g. root folder name */
  def getName(): String

  /** Get root path for the index */
  def getRoot(): String

  /** Get metadata for the index */
  def getMetadata(): Metadata

  /** Reference to catalog used to create or load index */
  def catalog: Catalog

  /** Whether or not index metadata matches provided spec */
  def containsSpec(spec: IndexSpec): Boolean

  /**
   * Append data to the index using provided spec and set of columns. Note that columns might not
   * match existing set of columns, and spec might include some options that were not part of
   * index when it was created. This method should provide mechanism of updating existing metadata
   * and merging new data into existing index data.
   */
  def append(spec: IndexSpec, columns: Seq[Column]): Unit

  /**
   * Search index using provided condition. If condition is not applicable, should throw exception,
   * since index cannot be utilized. If condition is partially supported then index should be
   * applied, and unsupported part of condition should be applied on returned DataFrame.
   */
  def search(condition: Column): DataFrame

  /**
   * Perform cleanup before deleting index. This might include dropping temporary directories,
   * invalidating internal cache, closing resources or database connection. Note that root directory
   * should not be deleted, catalog will remove it after this method. It is recommended that this
   * method catches and logs any exception, instead of throwing it upstream.
   */
  def delete(): Unit
}

/**
 * [[IndexSource]] is main entrypoint to either create or load [[Index]]. Each implementation should
 * provide at least those two methods, and optional fallback when index is not found but source is
 * supported.
 */
trait IndexSource {
  /**
   * Load index based on provided catalog and extracted metadata.
   */
  def loadIndex(catalog: Catalog, metadata: Metadata): Index

  /**
   * Create index based on provided spec and set of columns. Columns are not resolved, and may be
   * empty. `IndexSpec` is guaranteed to be provided for non-existent index, since save mode is
   * resolved by catalog. Should provide validation similar to `loadIndex(...)`. Passed directory is
   * guaranteed to exist.
   * Should return created index.
   */
  def createIndex(catalog: Catalog, spec: IndexSpec, dir: String, columns: Seq[Column]): Index

  /**
   * Fallback strategy to use when index is not found, but spec source is resolved. For example,
   * Parquet implementation might just load DataFrame using indexSpec provided path. Optional,
   * depends on implementation and, by default, throws unsupported exception.
   */
  def fallback(catalog: Catalog, spec: IndexSpec, condition: Column): DataFrame = {
    throw new UnsupportedOperationException()
  }
}
