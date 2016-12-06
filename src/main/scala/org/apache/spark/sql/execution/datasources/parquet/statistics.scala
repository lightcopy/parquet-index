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
    stats: ParquetColumnStatistics) {

  override def toString(): String = {
    val columnType = s"$repetition $fieldType ($originalType)".toLowerCase
    s"${getClass.getSimpleName}[name=$fieldName, type=$columnType, values=$valueCount, " +
    s"position=$startingPos, size=$size($compressedSize), stats=$stats]"
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

case class ParquetStatistics(
    path: String,
    len: Long,
    blocks: Array[ParquetBlockMetadata]) {

  override def toString(): String = {
    s"${getClass.getSimpleName}[path=$path, size=$len, blocks=${blocks.mkString("[", ", ", "]")}]"
  }
}
