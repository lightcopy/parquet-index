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

package com.github.lightcopy.index.parquet

/** Parquet file status, serialization-friendly */
case class ParquetFileStatus(path: String, size: Long)

/** Parquet statistics data */
case class ParquetFileStatistics(path: String, blocks: Seq[BlockStatistics]) {
  override def hashCode(): Int = path.hashCode

  override def equals(obj: Any): Boolean = {
    if (obj == null || !obj.isInstanceOf[ParquetFileStatistics]) return false
    val that = obj.asInstanceOf[ParquetFileStatistics]
    that.path == this.path
  }

  override def toString(): String = {
    s"${getClass.getSimpleName}(path=$path, numBlocks=${blocks.length})"
  }
}

/** Statistics for a row group */
case class BlockStatistics(
  rowCount: Long,
  startingPos: Long,
  compressedSize: Long,
  totalBytesSize: Long,
  columns: Map[String, ColumnStatistics])

/** Statistics for a column chunk */
case class ColumnStatistics(
  valueCount: Long,
  startingPos: Long,
  compressedSize: Long,
  totalBytesSize: Long,
  intStats: Option[IntStatistics],
  longStats: Option[LongStatistics],
  binaryStats: Option[BinaryStatistics],
  doubleStats: Option[DoubleStatistics])

/** Value range statistics */
abstract class ValueStatistics[T] {
  /** Value is within range, implementation should supply min and max */
  def withinRange(value: T): Boolean

  /** Number of null values collected */
  def getNumNulls(): Long
}

/** Integer column statistics */
case class IntStatistics(min: Int, max: Int, numNulls: Long)
  extends ValueStatistics[Int] {

  override def withinRange(value: Int): Boolean = {
    value >= min && value <= max
  }

  override def getNumNulls(): Long = numNulls
}

/** Long column statistics */
case class LongStatistics(min: Long, max: Long, numNulls: Long)
  extends ValueStatistics[Long] {

  override def withinRange(value: Long): Boolean = {
    value >= min && value <= max
  }

  override def getNumNulls(): Long = numNulls
}

/** Double column statistics */
case class DoubleStatistics(min: Double, max: Double, numNulls: Long)
  extends ValueStatistics[Double] {

  lazy val EPSILON: Double = {
    var eps: Double = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }

  override def withinRange(value: Double): Boolean = {
    (value > min && value < max) || (value - min <= EPSILON) || (value - max <= EPSILON)
  }

  override def getNumNulls(): Long = numNulls
}

/** Binary column statistics */
case class BinaryStatistics(min: Array[Byte], max: Array[Byte], numNulls: Long)
  extends ValueStatistics[Array[Byte]] {

  /** TODO: Implement comparison for binary column */
  override def withinRange(value: Array[Byte]): Boolean = {
    false
  }

  override def getNumNulls(): Long = numNulls
}
