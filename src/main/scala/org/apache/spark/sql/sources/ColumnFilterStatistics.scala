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

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}
import java.util.{HashSet => JHashSet}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter

import org.roaringbitmap.RoaringBitmap

/**
 * [[ColumnFilterStatistics]] is interface for column-based filters, provides some generic method
 * to read and write data, update/contains methods. Superclass keeps reference to the path to load
 * serialized data from, so subclasses do not need to implement that. Subclass must be serializable.
 */
abstract class ColumnFilterStatistics extends Serializable {
  // path to the serialized data for filter
  private var path: String = null
  // whether or not filter has been loaded, should be kept as transient
  @transient private var isFilterLoaded: Boolean = false

  /**
   * Update method for any supported type. Each column filter implementation should provide at least
   * one partial function for data type they handle. Input values are guaranteed to be non-null and
   * might be non-unique, e.g. filter might receive duplicate values. Note that this partial
   * function is called on already initialized filter.
   */
  protected def updateFunc: PartialFunction[Any, Unit]

  /**
   * Contains method for any supported type. Each column filter implementation should provide at
   * least one partial function. Input values are guaranteed to be non-null. When not sure if
   * filter contains value, return `true`. Note that method is called on already initialized filter.
   */
  protected def mightContainFunc: PartialFunction[Any, Boolean]

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
   * Update filter statistics with value that is not null.
   * Value is not guaranteed to be unique, and can be the same as previous method call.
   */
  def update(value: Any): Unit = {
    updateFunc.applyOrElse[Any, Unit](value, { case other =>
      throw new UnsupportedOperationException(s"$this does not support value $other")
    })
  }

  /**
   * Whether or not value is in column.
   * Provided parameter is non-null, defined value. When not sure if filter contains value, return
   * `true`. Method is called on already initialized filter only.
   */
  def mightContain(value: Any): Boolean = {
    mightContainFunc.applyOrElse[Any, Boolean](value, { case other =>
      throw new UnsupportedOperationException(s"$this does not support value $other")
    })
  }

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
  def isLoaded: Boolean = isFilterLoaded

  /** Mark this filter as loaded */
  def markLoaded: Unit = isFilterLoaded = true

  /**
   * Read data from disk to load filter. Operation is done once, so it is safe to call this method
   * many times, it will check if filter requires loading, otherwise will be no-op.
   */
  def readData(fs: FileSystem): Unit = {
    if (!isLoaded) {
      var in: InputStream = null
      try {
        in = fs.open(getPath)
        deserializeData(in)
        markLoaded
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
  val BLOOM_FILTER_TYPE = "bloom"
  val DICT_FILTER_TYPE = "dict"
  // Registered classes of filter statistics, key is a short name used in configuration
  val REGISTERED_FILTERS = Seq(BLOOM_FILTER_TYPE, DICT_FILTER_TYPE)

  /** Get column filter for provided filter type and date type */
  def getColumnFilter(
      dataType: DataType,
      filterType: String,
      blockRowCount: Long): ColumnFilterStatistics = {
    filterType match {
      case BLOOM_FILTER_TYPE =>
        BloomFilterStatistics(blockRowCount)
      case DICT_FILTER_TYPE =>
        dataType match {
          case IntegerType => BitmapFilterStatistics()
          case _ => DictionaryFilterStatistics()
        }
      case other =>
        sys.error(s"Unsupported filter statistics type $other, must be one of " +
          s"${REGISTERED_FILTERS.mkString("[", ", ", "]")}")
    }
  }
}

/**
 * [[FilterStatisticsMetadata]] maintains information about type and location of all column filter
 * statistics that are created during the computation of [[ParquetStatisticsRDD]].
 */
class FilterStatisticsMetadata {
  private var filterDirectory: FileStatus = null
  private var filterType: String = null

  /**
   * Set directory as root for all filter statistics for this job. Method always overwrites
   * previous value. If directory is None, then it is resolved as no directory provided, and will
   * disable filter statistics.
   */
  def setDirectory(dir: Option[FileStatus]): Unit = dir match {
    case Some(status) =>
      filterDirectory = status
    case None =>
      filterDirectory = null
  }

  /**
   * Set filter type from provided option. If value is None or is not registered, then filter type
   * is set to null, which will result in disabled filter statistics.
   * Type is validated against registered filters.
   */
  def setFilterType(tpe: Option[String]) = tpe match {
    case Some(value) if ColumnFilterStatistics.REGISTERED_FILTERS.contains(value) =>
      filterType = value
    case other =>
      filterType = null
  }

  /** Whether or not filter statistics are enabled for this metadata */
  def enabled: Boolean = {
    filterDirectory != null && filterType != null
  }

  /** Get directory. If disabled, will throw an exception */
  def getDirectory(): FileStatus = {
    require(enabled, s"Failed to extract directory for disabled metadata $this")
    filterDirectory
  }

  /** Shortcut to get Hadoop path instance from file status */
  def getPath(): Path = getDirectory.getPath

  /** Get filter type. If disabled, will throw an exception */
  def getFilterType(): String = {
    require(enabled, s"Failed to extract filter type for disabled metadata $this")
    filterType
  }

  override def toString(): String = {
    val content = if (enabled) s"directory=$filterDirectory, type=$filterType" else "none"
    s"(enabled=$enabled, $content)"
  }
}

////////////////////////////////////////////////////////////////
// Column filter statistics implementation
////////////////////////////////////////////////////////////////

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

  // Read and check of dates should be consistent with Spark reading of java.sql.Date and
  // java.sql.Timestamp. Currently we use DateTimeUtils.fromJavaDate and
  // DateTimeUtils.fromJavaTimestamp to convert date or timestamp into long value that we can store
  // in bloom filter
  override protected def updateFunc: PartialFunction[Any, Unit] = {
    case int: Int =>
      bloomFilter.put(int)
    case long: Long =>
      bloomFilter.put(long)
    case string: String =>
      bloomFilter.put(string)
    case date: java.sql.Date =>
      bloomFilter.put(DateTimeUtils.fromJavaDate(date))
    case time: java.sql.Timestamp =>
      bloomFilter.put(DateTimeUtils.fromJavaTimestamp(time))
  }

  // Similar to `updateFunc`, this method should be consistent with Spark reading of java.sql.Date
  // and java.sql.Timestamp.
  override protected def mightContainFunc: PartialFunction[Any, Boolean] = {
    case int: Int =>
      bloomFilter.mightContain(int)
    case long: Long =>
      bloomFilter.mightContain(long)
    case string: String =>
      bloomFilter.mightContain(string)
    case date: java.sql.Date =>
      bloomFilter.mightContain(DateTimeUtils.fromJavaDate(date))
    case time: java.sql.Timestamp =>
      bloomFilter.mightContain(DateTimeUtils.fromJavaTimestamp(time))
  }

  override protected def serializeData(out: OutputStream): Unit = {
    bloomFilter.writeTo(out)
  }

  override protected def deserializeData(in: InputStream): Unit = {
    bloomFilter = BloomFilter.readFrom(in)
  }

  /** Get number of rows, for testing */
  private[sources] def getNumRows(): Long = numRows
}

/**
 * [[DictionaryFilterStatistics]] provides filter statistics based on underlying lookup data
 * structure, in this case java.util.HashSet, to get exact answer on whether or not value is in
 * filter. Similar to bloom filter statistics, it is created per column.
 */
case class DictionaryFilterStatistics() extends ColumnFilterStatistics {
  @transient private var set = new JHashSet[Any]()

  /** Get new Spark configuration without loading defaults from system properties */
  private def getConf(): SparkConf = new SparkConf(false)

  override protected def updateFunc: PartialFunction[Any, Unit] = {
    case int: Int => set.add(int)
    case long: Long => set.add(long)
    case string: String => set.add(string)
    case date: java.sql.Date => set.add(date)
    case time: java.sql.Timestamp => set.add(time)
  }

  override protected def mightContainFunc: PartialFunction[Any, Boolean] = {
    case int: Int => set.contains(int)
    case long: Long => set.contains(long)
    case string: String => set.contains(string)
    case date: java.sql.Date => set.contains(date)
    case time: java.sql.Timestamp => set.contains(time)
  }

  override protected def serializeData(out: OutputStream): Unit = {
    val kryo = new KryoSerializer(getConf())
    val serializedStream = kryo.newInstance().serializeStream(out)
    try {
      serializedStream.writeObject(set)
    } finally {
      serializedStream.close()
    }
  }

  override protected def deserializeData(in: InputStream): Unit = {
    val kryo = new KryoSerializer(getConf())
    val deserializedStream = kryo.newInstance.deserializeStream(in)
    try {
      set = deserializedStream.readObject()
    } finally {
      // despite method closing external stream, we ensure that kryo stream is closed
      deserializedStream.close()
    }
  }

  /** Get value of `set`, for testing */
  private[sources] def getSet(): JHashSet[Any] = set
}

/**
 * [[BitmapFilterStatistics]] supports very efficient update and contains check on integer types.
 * Enabled for integer types as subtype of dictionary filter.
 */
case class BitmapFilterStatistics() extends ColumnFilterStatistics {
  @transient private var bitmap: RoaringBitmap = new RoaringBitmap()

  override protected def updateFunc: PartialFunction[Any, Unit] = {
    case int: Int => bitmap.add(int)
  }

  override protected def mightContainFunc: PartialFunction[Any, Boolean] = {
    case int: Int => bitmap.contains(int)
  }

  override protected def serializeData(out: OutputStream): Unit = {
    val dout = new DataOutputStream(out)
    try {
      bitmap.runOptimize()
      bitmap.serialize(dout)
    } finally {
      dout.close()
    }
  }

  override protected def deserializeData(in: InputStream): Unit = {
    val din = new DataInputStream(in)
    try {
      bitmap.deserialize(din)
    } finally {
      din.close()
    }
  }
}
