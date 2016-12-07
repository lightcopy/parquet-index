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
import org.apache.hadoop.fs.Path

import org.apache.spark.util.sketch.BloomFilter

////////////////////////////////////////////////////////////////
// == Parquet column statitics ==
////////////////////////////////////////////////////////////////

abstract class ParquetColumnStatistics {
  /** Return true, if provided value is of compatible type and within range between min and max */
  def contains(value: Any): Boolean

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
    s"${getClass.getSimpleName}[min=${getMin()}, max=${getMax()}, nulls=${getNumNulls()}]"
  }
}

case class ParquetIntStatistics(min: Int, max: Int, numNulls: Long)
  extends ParquetColumnStatistics {

  override def contains(value: Any): Boolean = value match {
    case intValue: Int =>
      intValue >= min && intValue <= max
    case other =>
      throw new UnsupportedOperationException(s"$this does not support $other, incompatible type")
  }

  override def getMin(): Any = min

  override def getMax(): Any = max

  override def getNumNulls(): Long  = numNulls
}

case class ParquetLongStatistics(min: Long, max: Long, numNulls: Long)
  extends ParquetColumnStatistics {

  override def contains(value: Any): Boolean = value match {
    case longValue: Long =>
      longValue >= min && longValue <= max
    case other =>
      throw new UnsupportedOperationException(s"$this does not support $other, incompatible type")
  }

  override def getMin(): Any = min

  override def getMax(): Any = max

  override def getNumNulls(): Long  = numNulls
}

case class ParquetStringStatistics(min: String, max: String, numNulls: Long)
  extends ParquetColumnStatistics {

  override def contains(value: Any): Boolean = value match {
    case stringValue: String =>
      stringValue >= min && stringValue <= max
    case other =>
      throw new UnsupportedOperationException(s"$this does not support $other, incompatible type")
  }

  override def getMin(): Any = min

  override def getMax(): Any = max

  override def getNumNulls(): Long  = numNulls
}

////////////////////////////////////////////////////////////////
// == Supported column filters for Parquet ==
////////////////////////////////////////////////////////////////

abstract class ParquetColumnFilter {
  /** Unique name of the filter */
  def identifier: String

  /** Initialize filter, e.g. load from disk */
  def init(conf: Configuration): Unit

  /** Destroy filter, release memory and/or resources */
  def destroy(): Unit

  /** Whether or not value is in column */
  def contains(value: Any): Boolean
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

  override def identifier: String = ParquetColumnFilter.BLOOM_FILTER

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

  override def destroy(): Unit = {
    bloomFilter = null
  }

  override def contains(value: Any): Boolean = {
    if (bloomFilter == null) {
      throw new IllegalStateException("Bloom filter is not initialized, call 'init()' first")
    }
    bloomFilter.mightContain(value)
  }

  override def toString(): String = {
    s"{id=$identifier, path=$path}"
  }
}

object ParquetColumnFilter {
  val BLOOM_FILTER = "bloom-filter"
}

////////////////////////////////////////////////////////////////
// == Parquet metadata for row group and column ==
////////////////////////////////////////////////////////////////

case class ParquetColumnMetadata(
    fieldName: String,
    repetition: String,
    fieldType: String,
    originalType: String,
    valueCount: Long,
    startingPos: Long,
    compressedSize: Long,
    size: Long,
    stats: ParquetColumnStatistics,
    private var filter: Option[ParquetColumnFilter]) {

  def setFilter(impl: ParquetColumnFilter): Unit = {
    this.filter = Option(impl)
  }

  override def toString(): String = {
    val columnType = s"$repetition $fieldType ($originalType)".toLowerCase
    s"${getClass.getSimpleName}[name=$fieldName, type=$columnType, values=$valueCount, " +
    s"position=$startingPos, size=$size($compressedSize), stats=$stats, filter=$filter]"
  }
}

case class ParquetBlockMetadata(
    rowCount: Long,
    startingPos: Long,
    compressedSize: Long,
    size: Long,
    columns: Map[String, ParquetColumnMetadata]) {

  override def toString(): String = {
    s"${getClass.getSimpleName}[rows=$rowCount, position=$startingPos, " +
    s"size=$size($compressedSize), columns=$columns]"
  }
}

/**
 * Global Parquet file statistics and metadata.
 * @param path fully-qualified path to the file
 * @param len length in bytes
 * @param indexSchema message type of Parquet index columns (subset of fileSchema)
 * @param fileSchema message type of Parquet file as string
 * @param blocks row groups statistics in file
 */
case class ParquetStatistics(
    path: String,
    len: Long,
    indexSchema: String,
    fileSchema: String,
    blocks: Array[ParquetBlockMetadata]) {

  def numRows(): Long = {
    if (blocks.isEmpty) 0 else blocks.map { _.rowCount }.sum
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}[path=$path, size=$len, schema=$fileSchema " +
    s"blocks=${blocks.mkString("[", ", ", "]")}]"
  }
}
