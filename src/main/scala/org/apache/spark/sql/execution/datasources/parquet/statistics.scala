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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter

import com.github.lightcopy.util.SerializableFileStatus

////////////////////////////////////////////////////////////////
// == Parquet column statitics ==
////////////////////////////////////////////////////////////////

abstract class ParquetColumnStatistics {
  /** Return true, if provided value is of compatible type and within range between min and max */
  def contains(value: Any): Boolean

  /** Return true, if max is less than value */
  def isLessThan(value: Any): Boolean

  /** Return true, if max is less than or equal to value */
  def isLessThanOrEqual(value: Any): Boolean

  /** Return true, if min is greater than value */
  def isGreaterThan(value: Any): Boolean

  /** Return true, if min is greater than or equal to value */
  def isGreaterThanOrEqual(value: Any): Boolean

  /** Get minimal value of column */
  def getMin(): Any

  /** Get maximum value of column */
  def getMax(): Any

  /** Get number of nulls in column */
  def getNumNulls(): Long

  /** Whether or not this column has null values */
  def hasNull(): Boolean = {
    getNumNulls > 0
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}(min=${getMin()}, max=${getMax()}, nulls=${getNumNulls()})"
  }
}

case class ParquetIntStatistics(min: Int, max: Int, numNulls: Long)
  extends ParquetColumnStatistics {

  require(min <= max, s"Min $min is expected to be less than or equal to max $max")
  require(numNulls >= 0, s"Number of nulls $numNulls is negative")

  override def contains(value: Any): Boolean = value match {
    case intValue: Int => intValue >= min && intValue <= max
    case other if other == null && hasNull => true
    case other => false
  }

  override def isLessThan(value: Any): Boolean = value match {
    case intValue: Int => max < intValue
    case other => false
  }

  override def isLessThanOrEqual(value: Any): Boolean = value match {
    case intValue: Int => max < intValue || max == intValue
    case other => false
  }

  override def isGreaterThan(value: Any): Boolean = value match {
    case intValue: Int => min > intValue
    case other => false
  }

  override def isGreaterThanOrEqual(value: Any): Boolean = value match {
    case intValue: Int => min > intValue || min == intValue
    case other => false
  }

  override def getMin(): Any = min

  override def getMax(): Any = max

  override def getNumNulls(): Long  = numNulls
}

case class ParquetLongStatistics(min: Long, max: Long, numNulls: Long)
  extends ParquetColumnStatistics {

  require(min <= max, s"Min $min is expected to be less than or equal to max $max")
  require(numNulls >= 0, s"Number of nulls $numNulls is negative")

  override def contains(value: Any): Boolean = value match {
    case longValue: Long => longValue >= min && longValue <= max
    case other if other == null && hasNull => true
    case other => false
  }

  override def isLessThan(value: Any): Boolean = value match {
    case longValue: Long => max < longValue
    case other => false
  }

  override def isLessThanOrEqual(value: Any): Boolean = value match {
    case longValue: Long => max < longValue || max == longValue
    case other => false
  }

  override def isGreaterThan(value: Any): Boolean = value match {
    case longValue: Long => min > longValue
    case other => false
  }

  override def isGreaterThanOrEqual(value: Any): Boolean = value match {
    case longValue: Long => min > longValue || min == longValue
    case other => false
  }

  override def getMin(): Any = min

  override def getMax(): Any = max

  override def getNumNulls(): Long  = numNulls
}

// Statistics use lexicographical order when comparing strings
// TODO: Add support for non-ASCII characters comparison, see
// https://issues.apache.org/jira/browse/SPARK-17213
case class ParquetStringStatistics(min: String, max: String, numNulls: Long)
  extends ParquetColumnStatistics {

  require(min != null, "Min value cannot be null, set null count instead")
  require(max != null, "Max value cannot be null, set null count instead")
  require(min <= max, s"Min $min is greater than max $max")
  require(numNulls >= 0, s"Number of nulls $numNulls is negative")

  override def contains(value: Any): Boolean = value match {
    case stringValue: String => stringValue >= min && stringValue <= max
    case other if other == null && hasNull => true
    case other => false
  }

  override def isLessThan(value: Any): Boolean = value match {
    case stringValue: String => max < stringValue
    case other => false
  }

  override def isLessThanOrEqual(value: Any): Boolean = value match {
    case stringValue: String => max < stringValue || max == stringValue
    case other => false
  }

  override def isGreaterThan(value: Any): Boolean = value match {
    case stringValue: String => min > stringValue
    case other => false
  }

  override def isGreaterThanOrEqual(value: Any): Boolean = value match {
    case stringValue: String => min > stringValue || min == stringValue
    case other => false
  }

  override def getMin(): Any = min

  override def getMax(): Any = max

  override def getNumNulls(): Long  = numNulls
}

////////////////////////////////////////////////////////////////
// == Supported column filters for Parquet ==
////////////////////////////////////////////////////////////////

abstract class ParquetColumnFilter {
  /** Initialize filter, e.g. load from disk */
  def init(conf: Configuration): Unit

  /** Whether or not filter requires calling init() method */
  def isSet(): Boolean

  /** Destroy filter, release memory and/or resources */
  def destroy(): Unit

  /** Whether or not value is in column */
  def mightContain(value: Any): Boolean
}

/**
 * Wrapper for `BloomFilter` for column values. Even if bloom filter is binary, e.g. not
 * specifically typed, we still maintain bloom filter per column.
 */
case class ParquetBloomFilter(path: String) extends ParquetColumnFilter {
  @transient private var bloomFilter: BloomFilter = null

  def setBloomFilter(filter: BloomFilter): Unit = {
    bloomFilter = filter
  }

  override def init(conf: Configuration): Unit = {
    if (bloomFilter == null) {
      val serde = new Path(path)
      val fs = serde.getFileSystem(conf)
      var in: InputStream = null
      try {
        in = fs.open(serde)
        bloomFilter = BloomFilter.readFrom(in)
      } finally {
        if (in != null) {
          in.close()
        }
      }
    }
  }

  override def isSet(): Boolean = bloomFilter != null

  override def destroy(): Unit = {
    bloomFilter = null
  }

  override def mightContain(value: Any): Boolean = {
    if (bloomFilter == null) {
      throw new IllegalStateException("Bloom filter is not initialized, call 'init()' first")
    }
    bloomFilter.mightContain(value)
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}(path=$path)"
  }
}

////////////////////////////////////////////////////////////////
// == Parquet metadata for row group and column ==
////////////////////////////////////////////////////////////////

/** Information about column statistics, including optional filter */
case class ParquetColumnMetadata(
    fieldName: String,
    valueCount: Long,
    stats: ParquetColumnStatistics,
    filter: Option[ParquetColumnFilter]) {

  /** Update current column metadata with new filter */
  def withFilter(newFilter: Option[ParquetColumnFilter]): ParquetColumnMetadata = {
    // do check on option value
    val updatedFilter = newFilter match {
      case Some(null) | None => None
      case valid @ Some(_) => valid
    }
    ParquetColumnMetadata(fieldName, valueCount, stats, updatedFilter)
  }
}

/** Block metadata stores information about indexed columns */
case class ParquetBlockMetadata(
    rowCount: Long,
    indexedColumns: Map[String, ParquetColumnMetadata])

/** Extended Parquet file status to preserve schema and file status */
case class ParquetFileStatus(
    status: SerializableFileStatus,
    fileSchema: String,
    blocks: Array[ParquetBlockMetadata]) {
  def numRows(): Long = {
    if (blocks.isEmpty) 0 else blocks.map { _.rowCount }.sum
  }
}

/** [[ParquetPartition]] is extended version of `Partition` in Spark */
case class ParquetPartition(
    values: InternalRow,
    files: Seq[ParquetFileStatus])

/**
 * [[ParquetIndexMetadata]] stores full information about Parquet table, including file path,
 * partitions and indexed columns, and used to reconstruct file index.
 */
case class ParquetIndexMetadata(
    tablePath: String,
    dataSchema: StructType,
    indexSchema: StructType,
    partitionSpec: PartitionSpec,
    partitions: Seq[ParquetPartition])
