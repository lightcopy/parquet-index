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

import java.util.Arrays

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata._
import org.apache.parquet.column.statistics.{IntStatistics => ParquetIntStatistics}
import org.apache.parquet.column.statistics.{LongStatistics => ParquetLongStatistics}
import org.apache.parquet.column.statistics.{BinaryStatistics => ParquetBinaryStatistics}
import org.apache.parquet.column.statistics.{DoubleStatistics => ParquetDoubleStatistics}

import org.apache.spark.{InterruptibleIterator, SparkContext, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import com.github.lightcopy.util.SerializableConfiguration

/** [[StatisticsPartition]] keeps information about file statuses for RDD */
class StatisticsPartition[T: ClassTag] (
    val rddId: Long,
    val slice: Int,
    val schema: StructType,
    val values: Seq[T])
  extends Partition with Serializable {

  def iterator: Iterator[T] = values.iterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: StatisticsPartition[_] => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice
}

/**
 * [[StatisticsRDD]] collects various statistics on each Parquet file. Uses distributed Hadoop
 * configuration to read file on each executor.
 */
class StatisticsRDD(
    @transient sc: SparkContext,
    _conf: Configuration,
    schema: StructType,
    data: Seq[ParquetFileStatus],
    numPartitions: Int)
  extends RDD[ParquetFileStatistics](sc, Nil) {

  private val confBroadcast = sparkContext.broadcast(new SerializableConfiguration(_conf))

  def hadoopConf: Configuration = confBroadcast.value.value

  override def getPartitions: Array[Partition] = {
    val slices = StatisticsRDD.partitionData(data, numPartitions)
    slices.indices.map { i =>
      new StatisticsPartition[ParquetFileStatus](id, i, schema, slices(i))
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ParquetFileStatistics] = {
    val configuration = hadoopConf
    val buffer = new ArrayBuffer[ParquetFileStatistics]()
    val partition = split.asInstanceOf[StatisticsPartition[ParquetFileStatistics]]

    for (elem <- partition.iterator) {
      val path = new Path(elem.path)
      val fs = path.getFileSystem(configuration)
      val status = fs.getFileStatus(path)
      val footer = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(configuration,
        Arrays.asList(status), false).get(0)
      val parquetMetadata = footer.getParquetMetadata
      buffer.append(gatherStats(status.getPath, parquetMetadata, partition.schema))
    }

    new InterruptibleIterator(context, buffer.toIterator)
  }

  /** Convert Parquet metadata into file statistics */
  private def gatherStats(
      path: Path,
      metadata: ParquetMetadata,
      schema: StructType): ParquetFileStatistics = {
    ParquetFileStatistics(path.toString,
      collectBlocks(metadata, schema))
  }

  private def collectBlocks(
      metadata: ParquetMetadata,
      schema: StructType): Seq[BlockStatistics] = {
    metadata.getBlocks().asScala.map { block =>
      val columns = collectColumns(block, schema)
      BlockStatistics(block.getRowCount, block.getStartingPos, block.getCompressedSize,
        block.getTotalByteSize, columns)
    }
  }

  private def collectColumns(
      bm: BlockMetaData,
      schema: StructType): Map[String, ColumnStatistics] = {
    val map = schema.map { field =>
      (ColumnPath.get(field.name), field)
    }.toMap

    bm.getColumns.asScala.flatMap(chunk => map.get(chunk.getPath) match {
      case Some(field) =>
        val valueCount = chunk.getValueCount
        val startingPos = chunk.getStartingPos
        val compressedSize = chunk.getTotalSize
        val totalBytesSize = chunk.getTotalUncompressedSize
        val stats = field.dataType match {
          case IntegerType =>
            val intStats = getIntStatistics(chunk)
            ColumnStatistics(valueCount, startingPos, compressedSize, totalBytesSize,
              Some(intStats), None, None, None)
          case LongType =>
            val longStats = getLongStatistics(chunk)
            ColumnStatistics(valueCount, startingPos, compressedSize, totalBytesSize,
              None, Some(longStats), None, None)
          case StringType =>
            val binaryStats = getBinaryStatistics(chunk)
            ColumnStatistics(valueCount, startingPos, compressedSize, totalBytesSize,
              None, None, Some(binaryStats), None)
          case DoubleType =>
            val doubleStats = getDoubleStatistics(chunk)
            ColumnStatistics(valueCount, startingPos, compressedSize, totalBytesSize,
              None, None, None, Some(doubleStats))
          case other =>
            throw new UnsupportedOperationException(s"Unsupported schema field $other")
        }
        Some((field.name, stats))
      case None =>
        None
    }).toMap
  }

  private def getIntStatistics(chunk: ColumnChunkMetaData): IntStatistics = {
    val chunkStats = chunk.getStatistics.asInstanceOf[ParquetIntStatistics]
    IntStatistics(chunkStats.getMin, chunkStats.getMax, chunkStats.getNumNulls)
  }

  private def getLongStatistics(chunk: ColumnChunkMetaData): LongStatistics = {
    val chunkStats = chunk.getStatistics.asInstanceOf[ParquetLongStatistics]
    LongStatistics(chunkStats.getMin, chunkStats.getMax, chunkStats.getNumNulls)
  }

  private def getBinaryStatistics(chunk: ColumnChunkMetaData): BinaryStatistics = {
    val chunkStats = chunk.getStatistics.asInstanceOf[ParquetBinaryStatistics]
    BinaryStatistics(chunkStats.getMin.getBytes, chunkStats.getMax.getBytes, chunkStats.getNumNulls)
  }

  private def getDoubleStatistics(chunk: ColumnChunkMetaData): DoubleStatistics = {
    val chunkStats = chunk.getStatistics.asInstanceOf[ParquetDoubleStatistics]
    DoubleStatistics(chunkStats.getMin, chunkStats.getMax, chunkStats.getNumNulls)
  }
}

private[parquet] object StatisticsRDD {
  /** Partition data into sequence of buckets with values based on provided number of partitions */
  def partitionData[T: ClassTag](data: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    require(numSlices >= 1, s"Positive number of slices required, found $numSlices")

    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }

    val array = data.toArray
    positions(array.length, numSlices).map { case (start, end) =>
      array.slice(start, end).toSeq }.toSeq
  }
}
