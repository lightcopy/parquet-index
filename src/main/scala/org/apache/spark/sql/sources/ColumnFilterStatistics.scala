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

package org.apache.spark.sql.sources

import java.io.{InputStream, OutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.util.sketch.BloomFilter

/**
 * [[ColumnFilterStatistics]] is interface for column-based filters, provides some generic method
 * to read and write data, update/contains methods. Superclass keeps reference to the path to load
 * serialized data from, so subclasses do not need to implement that. Subclass must be serializable.
 */
abstract class ColumnFilterStatistics extends Serializable {
  // path to the serialized data for filter
  private var path: String = null

  /**
   * Update filter statistics with value that is not null.
   * Value is not guaranteed to be unique, and can be the same as previous method call.
   */
  def update(value: Any): Unit

  /**
   * Whether or not value is in column.
   * Provided parameter is non-null, defined value. When not sure if filter contains value, return
   * `true`. Method is called on already initialized filter only.
   */
  def mightContain(value: Any): Boolean

  /**
   * Whether or not filter requires loading serialized data.
   * Can be called multiple times to determine state of filter.
   */
  protected def needToReadData(): Boolean

  /**
   * Serialize data into output stream. For example, bloom filter statistics can write bloom filter
   * into stream. Note that in this case bloom filter should be transient. This method is called
   * once after filter statistics is collected.
   */
  protected def serializeData(out: OutputStream): Unit

  /**
   * Deserialize data from input stream. For example, initialize bloom filter. This method might
   * be called many times depending on `needToReadData` method. Do not need to close stream, it is
   * handled upstream.
   */
  protected def deserializeData(in: InputStream): Unit

  /**
   * Set path for serialized data, when done writing to disk.
   * Also checks for potentially invalid path.
   */
  def setPath(path: Path): Unit = {
    require(path != null, s"Incorrect serialization path $path")
    this.path = path.toString
  }

  /**
   * Get path to load data.
   */
  def getPath: Path = {
    new Path(this.path)
  }

  /**
   * Whether or not filter is loaded into memory, implemented as function instead of lazy val.
   * Added for testing purposes.
   */
  private[sql] def isLoaded(): Boolean = {
    !needToReadData()
  }

  /**
   * Read data from disk to load filter. Operation is done once, so it is safe to call this method
   * many times, it will check if filter requires loading, otherwise will be no-op.
   */
  def readData(fs: FileSystem): Unit = {
    if (needToReadData) {
      var in: InputStream = null
      try {
        in = fs.open(getPath)
        deserializeData(in)
      } finally {
        if (in != null) {
          in.close()
        }
      }
    }
  }

  /**
   * Write filter data onto disk. Method is called only once, when filter is updated with all
   * available data.
   */
  def writeData(fs: FileSystem): Unit = {
    var out: OutputStream = null
    try {
      out = fs.create(getPath, false)
      serializeData(out)
    } finally {
      if (out != null) {
        out.close()
      }
    }
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}"
  }
}

object ColumnFilterStatistics {
  // Registered classes of filter statistics, key is a short name used in configuration
  val REGISTERED_FILTERS = Map("bloom" -> classOf[BloomFilterStatistics])

  /**
   * Get [[ColumnFilterStatistics]] class for short name, used for selecting filter type in
   * configuration. If short name is not found, throws runtime exception; note that name string is
   * not modified to find a match (compared as is).
   */
  def classForName(name: String): Class[_] = {
    REGISTERED_FILTERS.getOrElse(name, sys.error(
      s"Unsupported filter statistics type $name, must be one of " +
      s"${REGISTERED_FILTERS.keys.mkString("[", ", ", "]")}"))
  }
}

/**
 * [[BloomFilterStatistics]] provides filter statistics based on bloom filter per column.
 * Number of expected records `numRows` should only be set when updating records. For read-contains
 * workflow, number of records is not required, because data will be loaded from disk.
 */
case class BloomFilterStatistics(numRows: Long = 1024L) extends ColumnFilterStatistics {
  require(numRows > 0, s"Invalid expected number of records $numRows, should be > 0")
  @transient private var bloomFilter: BloomFilter =
    try {
      // Create bloom filter with at most ~1048576 expected records and FPP of 3%
      BloomFilter.create(Math.min(numRows, 1 << 20), 0.03)
    } catch {
      case oom: OutOfMemoryError =>
        throw new OutOfMemoryError(s"Failed to create bloom filter for numRows=$numRows. " +
          "Consider reducing partition size and/or number of filters/indexed columns").
          initCause(oom)
    }
  @transient private var hasLoadedData: Boolean = false

  override def update(value: Any): Unit = bloomFilter.put(value)

  override def mightContain(value: Any): Boolean = bloomFilter.mightContain(value)

  override protected def needToReadData(): Boolean = !hasLoadedData

  override protected def serializeData(out: OutputStream): Unit = {
    bloomFilter.writeTo(out)
  }

  override protected def deserializeData(in: InputStream): Unit = {
    bloomFilter = BloomFilter.readFrom(in)
    hasLoadedData = true
  }

  /** Get number of rows, for testing */
  private[sources] def getNumRows(): Long = numRows

  /** Get value of `hasLoadedData`, for testing */
  private[sources] def getHasLoadedData(): Boolean = hasLoadedData
}
