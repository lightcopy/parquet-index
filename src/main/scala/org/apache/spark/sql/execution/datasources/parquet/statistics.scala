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

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.{JsonMethods => JSON}

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

  /** Convert to JSON */
  def toJsonObj(): JValue

  def toJSON(): String = {
    JSON.compact(JSON.render(toJsonObj()))
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

  override def toJsonObj(): JValue = {
    ("statistics" -> ParquetColumnStatistics.INT_TYPE) ~
      ("min" -> min) ~ ("max" -> max) ~ ("nulls" -> numNulls)
  }
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

  override def toJsonObj(): JValue = {
    ("statistics" -> ParquetColumnStatistics.LONG_TYPE) ~
      ("min" -> min) ~ ("max" -> max) ~ ("nulls" -> numNulls)
  }
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

  override def toJsonObj(): JValue = {
    ("statistics" -> ParquetColumnStatistics.STRING_TYPE) ~
      ("min" -> min) ~ ("max" -> max) ~ ("nulls" -> numNulls)
  }
}

object ParquetColumnStatistics {
  val INT_TYPE = "int"
  val LONG_TYPE = "long"
  val STRING_TYPE = "string"

  def fromJsonObj(value: JValue): ParquetColumnStatistics = value match {
    case JObject(
      JField("statistics", JString(INT_TYPE)) ::
      JField("min", JInt(min)) ::
      JField("max", JInt(max)) ::
      JField("nulls", JInt(numNulls)) :: Nil) =>
        ParquetIntStatistics(min.toInt, max.toInt, numNulls.toLong)
    case JObject(
      JField("statistics", JString(LONG_TYPE)) ::
      JField("min", JInt(min)) ::
      JField("max", JInt(max)) ::
      JField("nulls", JInt(numNulls)) :: Nil) =>
        ParquetLongStatistics(min.toLong, max.toLong, numNulls.toLong)
    case JObject(
      JField("statistics", JString(STRING_TYPE)) ::
      JField("min", JString(min)) ::
      JField("max", JString(max)) ::
      JField("nulls", JInt(numNulls)) :: Nil) =>
        ParquetStringStatistics(min, max, numNulls.toLong)
    case other => throw new UnsupportedOperationException(
      s"Could not convert json $other to ParquetColumnStatistics")
  }
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

  /** Convert into JSON */
  def toJsonObj(): JValue

  def toJSON(): String = {
    JSON.compact(JSON.render(toJsonObj()))
  }
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

  override def toJsonObj(): JValue = {
    ("filter" -> identifier) ~ ("path" -> path)
  }

  override def toString(): String = {
    s"{id=$identifier, path=$path}"
  }
}

object ParquetColumnFilter {
  val BLOOM_FILTER = "bloom-filter"

  def fromJsonObj(value: JValue): ParquetColumnFilter = value match {
    case JObject(
        JField("filter", JString(BLOOM_FILTER)) ::
        JField("path", JString(path)) :: Nil) =>
      ParquetBloomFilter(path)
    case other => throw new UnsupportedOperationException(
      s"Could not convert json $other to ParquetColumnFilter")
  }
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

  private[parquet] def toJsonObj(): JValue = {
    val filterJson = filter.map(_.toJsonObj)
    ("fieldName" -> fieldName) ~
      ("repetition" -> repetition) ~
        ("fieldType" -> fieldType) ~
          ("originalType" -> originalType) ~
            ("valueCount" -> valueCount) ~
              ("startingPos" -> startingPos) ~
                ("compressedSize" -> compressedSize) ~
                  ("size" -> size) ~
                    ("stats" -> stats.toJsonObj) ~
                      ("filter" -> filterJson)
  }

  def toJSON(): String = {
    JSON.compact(JSON.render(toJsonObj()))
  }

  def setFilter(impl: ParquetColumnFilter): Unit = {
    this.filter = Option(impl)
  }

  def getFilter(): Option[ParquetColumnFilter] = {
    this.filter
  }

  override def toString(): String = {
    val columnType = s"$repetition $fieldType ($originalType)".toLowerCase
    s"${getClass.getSimpleName}[name=$fieldName, type=$columnType, values=$valueCount, " +
    s"position=$startingPos, size=$size($compressedSize), stats=$stats, filter=$filter]"
  }
}

object ParquetColumnMetadata {
  def fromJsonObj(value: JValue) = value match {
    case JObject(
        JField("fieldName", JString(fieldName)) ::
        JField("repetition", JString(repetition)) ::
        JField("fieldType", JString(fieldType)) ::
        JField("originalType", originalTypeValue) ::
        JField("valueCount", JInt(valueCount)) ::
        JField("startingPos", JInt(startingPos)) ::
        JField("compressedSize", JInt(compressedSize)) ::
        JField("size", JInt(size)) ::
        JField("stats", stats: JObject) :: maybeFilter) =>
      val resolvedStats = ParquetColumnStatistics.fromJsonObj(stats)
      val resolvedFilter = maybeFilter match {
        case JField("filter", filterData: JObject) :: Nil =>
          Some(ParquetColumnFilter.fromJsonObj(filterData))
        case Nil => None
      }
      val resolvedOrigType: String = originalTypeValue match {
        case JString(actualValue) => actualValue
        case JNull => null // for INT64
        case other => throw new UnsupportedOperationException(s"Unknown original type $other")
      }

      ParquetColumnMetadata(fieldName, repetition, fieldType, resolvedOrigType, valueCount.toLong,
        startingPos.toLong, compressedSize.toLong, size.toLong, resolvedStats, resolvedFilter)
    case other => throw new UnsupportedOperationException(
      s"Could not convert json $other to ParquetColumnMetadata")
  }
}

case class ParquetBlockMetadata(
    rowCount: Long,
    startingPos: Long,
    compressedSize: Long,
    size: Long,
    columns: Map[String, ParquetColumnMetadata]) {

  private[parquet] def toJsonObj(): JValue = {
    ("rowCount" -> rowCount) ~
      ("startingPos" -> startingPos) ~
        ("compressedSize" -> compressedSize) ~
          ("size" -> size) ~
            ("columns" -> columns.mapValues(_.toJsonObj))
  }

  def toJSON(): String = {
    JSON.compact(JSON.render(toJsonObj()))
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}[rows=$rowCount, position=$startingPos, " +
    s"size=$size($compressedSize), columns=$columns]"
  }
}

object ParquetBlockMetadata {
  def fromJsonObj(value: JValue): ParquetBlockMetadata = value match {
    case JObject(
        JField("rowCount", JInt(rowCount)) ::
        JField("startingPos", JInt(startingPos)) ::
        JField("compressedSize", JInt(compressedSize)) ::
        JField("size", JInt(size)) ::
        JField("columns", JObject(columns)) :: Nil) =>
      val resolvedColumns = columns.map { case JField(fieldName, columnMetadata) =>
        (fieldName, ParquetColumnMetadata.fromJsonObj(columnMetadata))
      }.toMap

      ParquetBlockMetadata(rowCount.toLong, startingPos.toLong, compressedSize.toLong,
        size.toLong, resolvedColumns)
    case other => throw new UnsupportedOperationException(
      s"Could not convert json $other to ParquetBlockMetadata")
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

  private[parquet] def toJsonObj(): JValue = {
    val blocksJson = blocks.map(_.toJsonObj).toList
    ("path" -> path) ~
      ("len" -> len) ~
        ("indexSchema" -> indexSchema) ~
          ("fileSchema" -> fileSchema) ~
            ("blocks" -> blocksJson)
  }

  def toJSON(): String = {
    JSON.compact(JSON.render(toJsonObj()))
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}[path=$path, size=$len, schema=$fileSchema " +
    s"blocks=${blocks.mkString("[", ", ", "]")}]"
  }
}

object ParquetStatistics {
  implicit private val formats = org.json4s.DefaultFormats

  def fromJSON(json: String): ParquetStatistics = {
    fromJsonObj(JSON.parse(json))
  }

  def fromJsonObj(value: JValue): ParquetStatistics = value match {
    case JObject(
        JField("path", JString(path)) ::
        JField("len", JInt(len)) ::
        JField("indexSchema", JString(indexSchema)) ::
        JField("fileSchema", JString(fileSchema)) ::
        JField("blocks", JArray(blocks)) :: Nil) =>
      val resolvedBlocks = blocks.map(ParquetBlockMetadata.fromJsonObj).toArray
      ParquetStatistics(path, len.toLong, indexSchema, fileSchema, resolvedBlocks)
    case other => throw new UnsupportedOperationException(
      s"Could not convert json $other to ParquetStatistics")
  }
}
