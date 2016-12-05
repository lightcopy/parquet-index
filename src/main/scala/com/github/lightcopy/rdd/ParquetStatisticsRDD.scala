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

package com.github.lightcopy.rdd

import java.util.Arrays

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{InterruptibleIterator, SparkContext, Partition, TaskContext}
import org.apache.spark.rdd.RDD

import com.github.lightcopy.util.SerializableConfiguration

/**
 * [[ParquetFileStatus]] is seiralizable version of `org.apache.hadoop.fs.FileStatus`, it is
 * required to be a valid Parquet file. Normally obtained after listing files before scanning files
 * in `ParquetRelation`.
 * @param path fully-qualified path to the Parquet file
 * @param len total size in bytes
 * @param owner file owner
 * @param group file group
 */
case class ParquetFileStatus(path: String, len: Long, owner: String, group: String)

/** [[ParquetFileStatusPartition]] to keep information about file statuses */
private[rdd] class ParquetFileStatusPartition (
    val rddId: Long,
    val slice: Int,
    val values: Seq[ParquetFileStatus])
  extends Partition with Serializable {

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ParquetFileStatusPartition => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  def iterator: Iterator[ParquetFileStatus] = values.iterator
}

/**
 * [[ParquetStatisticsRDD]] collects various statistics on each Parquet file. Currently partitioning
 * columns cannot be part of statistics, and selected index columns must be part of each Parquet
 * schema (which means merged schemas are not supported). Uses distributed Hadoop configuration to
 * read file on each executor.
 */
class ParquetStatisticsRDD(
    @transient sc: SparkContext,
    configuration: Configuration,
    data: Seq[ParquetFileStatus],
    numPartitions: Int)
  extends RDD[Int](sc, Nil) {

  private val confBroadcast = sparkContext.broadcast(new SerializableConfiguration(configuration))

  def hadoopConf: Configuration = confBroadcast.value.value

  override def getPartitions: Array[Partition] = {
    val slices = ParquetStatisticsRDD.partitionData(data, numPartitions)
    slices.indices.map { i =>
      new ParquetFileStatusPartition(id, i, slices(i))
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
    val configuration = hadoopConf
    val partition = split.asInstanceOf[ParquetFileStatusPartition]
    for (elem <- partition.iterator) { }
    null
  }
}

private[rdd] object ParquetStatisticsRDD {
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
