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

import java.util.Arrays

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.parquet.column.statistics._
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.schema.MessageType

import org.apache.spark.{SparkContext, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import com.github.lightcopy.util.SerializableConfiguration

/**
 * [[ParquetFileStatus]] is seiralizable version of `org.apache.hadoop.fs.FileStatus`, it is
 * required to be a valid Parquet file. Normally obtained after listing files before scanning files
 * in `ParquetRelation`.
 * @param path fully-qualified path to the Parquet file
 * @param len total size in bytes
 */
case class ParquetFileStatus(path: String, len: Long)

/** [[ParquetFileStatusPartition]] to keep information about file statuses */
private[parquet] class ParquetFileStatusPartition (
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
 * @param sc Spark context
 * @param configuration Hadoop configuration, normally 'sc.hadoopConfiguration'
 * @param schema columns to compute index for
 * @param data list of Parquet file statuses
 * @param numPartitions number of partitions on use
 */
class ParquetStatisticsRDD(
    @transient private val sc: SparkContext,
    @transient private val configuration: Configuration,
    schema: StructType,
    data: Seq[ParquetFileStatus],
    numPartitions: Int)
  extends RDD[ParquetStatistics](sc, Nil) {

  private val confBroadcast =
    sparkContext.broadcast(new SerializableConfiguration(configuration))

  // validate Spark SQL schema, schema should be subset of Parquet file message type and be one of
  // supported types
  ParquetStatisticsRDD.validateStructType(schema)

  def hadoopConf: Configuration = confBroadcast.value.value

  override def getPartitions: Array[Partition] = {
    val slices = ParquetStatisticsRDD.partitionData(data, numPartitions)
    slices.indices.map { i =>
      new ParquetFileStatusPartition(id, i, slices(i))
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ParquetStatistics] = {
    val configuration = hadoopConf
    val partition = split.asInstanceOf[ParquetFileStatusPartition]
    // convert schema of struct type into Parquet schema
    val requestedSchema = new ParquetSchemaConverter().convert(schema)
    logDebug(s"Indexed schema ${schema.prettyJson}, parquet schema $requestedSchema")
    val iter = partition.iterator

    new Iterator[ParquetStatistics]() {
      override def hasNext: Boolean = {
        iter.hasNext
      }

      override def next(): ParquetStatistics = {
        val path = new Path(iter.next.path)
        val fs = path.getFileSystem(configuration)
        val status = fs.getFileStatus(path)
        logInfo(s"Reading file ${status.getPath}")
        // read metadata for the file, we always extract one footer
        val footer = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(
          configuration, Arrays.asList(status), false).get(0)
        val metadata = footer.getParquetMetadata
        val schema = metadata.getFileMetaData.getSchema
        // check that requested schema is part of the file schema
        schema.checkContains(requestedSchema)

        logInfo(s"""
          | Collect statistics for Parquet file:
          | ${status.getPath}
          | == Schema ==
          | $schema
          | == Requested schema ==
          | $requestedSchema
          """.stripMargin)

        // convert current file metadata with requested columns into Parquet statistics
        // currently global Parquet schema is merged and inferred on a driver, which is suboptimal
        // since we can reduce schema during metadata collection.
        // TODO: Partially merge schema during each task
        val blocks = ParquetStatisticsRDD.convert(metadata, requestedSchema)
        ParquetStatistics(status.getPath.toString, status.getLen, schema.toString, blocks)
      }
    }
  }
}

private[parquet] object ParquetStatisticsRDD {
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

  /**
   * Validate input schema as `StructType` and throw exception if schema does not match expected
   * column types. Currently only IntegerType, LongType, and StringType are supported. Note that
   * this is used in statistics conversion, so when adding new type, one should update statistics.
   */
  def validateStructType(schema: StructType): Unit = {
    // supported data types from Spark SQL
    val supportedTypes: Set[DataType] = Set(IntegerType, LongType, StringType)
    schema.fields.foreach { field =>
      if (!supportedTypes.contains(field.dataType)) {
        throw new UnsupportedOperationException("Schema contains unsupported type, " +
          s"field=$field, supported types=${supportedTypes.mkString("[", ", ", "]")}")
      }
    }
  }

  /**
   * Extract statistics from Parquet file metadata for requested schema, returns array of block
   * metadata (for each row group).
   */
  def convert(metadata: ParquetMetadata, schema: MessageType): Array[ParquetBlockMetadata] = {
    val blocks = metadata.getBlocks().asScala.map { block =>
      ParquetBlockMetadata(block.getRowCount, block.getStartingPos, block.getCompressedSize,
        block.getTotalByteSize, convertColumns(block.getColumns().asScala, schema))
    }
    blocks.toArray
  }

  private def convertColumns(
      columns: Seq[ColumnChunkMetaData],
      schema: MessageType): Map[String, ParquetColumnMetadata] = {
    val seq = columns.flatMap { column =>
      // process column metadata if column is a part of the schema
      if (schema.containsPath(column.getPath.toArray)) {
        // normalize path as "a.b.c"
        val dotString = column.getPath.toDotString
        val fieldIndex = schema.getFieldIndex(dotString)
        // repetition level
        val repetition = schema.getType(fieldIndex).getRepetition.name
        // original type for column, can be null in case of INT64
        val originalType = Option(schema.getType(fieldIndex).getOriginalType) match {
          case Some(tpe) => tpe.name
          case None => null
        }
        // convert min-max statistics into internal format
        val columnMinMaxStats = convertStatistics(column.getStatistics)
        Some(ParquetColumnMetadata(
          dotString,
          repetition,
          column.getType.name,
          originalType,
          column.getValueCount,
          column.getStartingPos,
          column.getTotalSize,
          column.getTotalUncompressedSize,
          columnMinMaxStats,
          None))
      } else {
        None
      }
    }

    seq.map { meta => (meta.fieldName, meta) }.toMap
  }

  private def convertStatistics(parquetStatistics: Statistics[_]): ParquetColumnStatistics = {
    parquetStatistics match {
      case stats: IntStatistics =>
        ParquetIntStatistics(stats.getMin, stats.getMax, stats.getNumNulls)
      case stats: LongStatistics =>
        ParquetLongStatistics(stats.getMin, stats.getMax, stats.getNumNulls)
      case stats: BinaryStatistics =>
        // we assume that binary statistics is always string, this check is done for StructType
        // columns before running RDD, see `validateStructType(...)`
        ParquetStringStatistics(stats.getMin.toStringUsingUTF8, stats.getMax.toStringUsingUTF8,
          stats.getNumNulls)
      case other =>
        throw new UnsupportedOperationException(s"Statistics $other is not supported, see " +
          "information on what types are supported by index")
    }
  }
}
