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

import java.io.IOException
import java.util.{Arrays, UUID}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.parquet.column.statistics._
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputSplit, ParquetRecordReader}
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.schema.MessageType

import org.apache.spark.{SparkContext, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter

import com.github.lightcopy.util.{SerializableConfiguration, SerializableFileStatus}

/** [[ParquetStatisticsPartition]] to keep information about file statuses */
private[parquet] class ParquetStatisticsPartition(
    val rddId: Long,
    val slice: Int,
    val values: Seq[SerializableFileStatus])
  extends Partition with Serializable {

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ParquetStatisticsPartition => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  def iterator: Iterator[SerializableFileStatus] = values.iterator
}

/**
 * [[ParquetStatisticsRDD]] collects various statistics on each Parquet file. Currently partitioning
 * columns cannot be part of statistics, and selected index columns must be part of each Parquet
 * schema (which means merged schemas are not supported). Uses distributed Hadoop configuration to
 * read file on each executor.
 * @param sc Spark context
 * @param hadoopConf Hadoop configuration, normally 'sc.hadoopConfiguration'
 * @param schema columns to compute index for
 * @param data list of Parquet file statuses
 * @param numPartitions number of partitions on use
 */
class ParquetStatisticsRDD(
    @transient private val sc: SparkContext,
    @transient private val hadoopConf: Configuration,
    schema: StructType,
    data: Seq[SerializableFileStatus],
    numPartitions: Int)
  extends RDD[ParquetFileStatus](sc, Nil) {

  private val confBroadcast =
    sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

  // validate Spark SQL schema, schema should be subset of Parquet file message type and be one of
  // supported types
  ParquetSchemaUtils.validateStructType(schema)

  def hadoopConfiguration: Configuration = confBroadcast.value.value

  override def getPartitions: Array[Partition] = {
    val slices = ParquetStatisticsRDD.partitionData(data, numPartitions)
    slices.indices.map { i =>
      new ParquetStatisticsPartition(id, i, slices(i))
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ParquetFileStatus] = {
    val configuration = hadoopConfiguration
    val partition = split.asInstanceOf[ParquetStatisticsPartition]
    // convert schema of struct type into Parquet schema
    val partitionIndex = partition.index
    val requestedSchema = new ParquetSchemaConverter().convert(schema)
    logInfo(s"Indexed schema ${schema.prettyJson}, parquet schema $requestedSchema")
    val iter = partition.iterator

    new Iterator[ParquetFileStatus]() {
      override def hasNext: Boolean = {
        iter.hasNext
      }

      override def next(): ParquetFileStatus = {
        val serdeStatus = iter.next
        val status = SerializableFileStatus.toFileStatus(serdeStatus)
        logDebug(s"Reading file ${status.getPath}")
        // read metadata for the file, we always extract one footer
        val footer = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(
          configuration, Arrays.asList(status), false).get(0)
        val metadata = footer.getParquetMetadata
        val schema = metadata.getFileMetaData.getSchema
        // check that requested schema is part of the file schema
        schema.checkContains(requestedSchema)

        logDebug(s"""
          | Collect statistics for Parquet file:
          | ${status.getPath}
          | == Schema ==
          | $schema
          | == Requested schema ==
          | $requestedSchema
          """.stripMargin)

        // check if we need to compute bloom filters for each index column, currently there is no
        // selection on different index on a column, as well as no subset indexing. If path is
        // is not defined then bloom filter is disabled
        val bloomFilterDir = Option(configuration.get(ParquetMetastoreSupport.BLOOM_FILTER_DIR)).
          map { path =>
            val filterPath = new Path(path)
            filterPath.getFileSystem(configuration).getFileStatus(filterPath)
          }

        // convert current file metadata with requested columns into Parquet statistics as list of
        // blocks with already resolved filter (if any).
        // currently global Parquet schema is merged and inferred on a driver, which is suboptimal
        // since we can reduce schema during metadata collection.
        // TODO: Partially merge schema during each task
        // TODO: Currently partition index is hacky way of specifying unique output file name,
        // should replace it or inject in a better way
        val blocks = ParquetStatisticsRDD.convertBlocks(metadata, requestedSchema)

        val updatedBlocks = if (bloomFilterDir.isDefined) {
          logDebug(s"Bloom filter root status: $bloomFilterDir, requested schema: $requestedSchema")
          ParquetStatisticsRDD.withBloomFilters(
            requestedSchema,
            ParquetStatisticsRDD.taskAttemptContext(configuration, partitionIndex),
            blocks,
            status,
            bloomFilterDir.get)
        } else {
          logDebug(s"Bloom filter is disabled")
          blocks
        }

        ParquetFileStatus(serdeStatus, schema.toString, updatedBlocks)
      }
    }
  }
}

private[parquet] object ParquetStatisticsRDD {
  // Supported top-level Spark SQL data types
  val SUPPORTED_TYPES: Set[DataType] = Set(IntegerType, LongType, StringType)

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
   * Extract statistics from Parquet file metadata for requested schema, returns array of block
   * metadata (for each row group).
   */
  def convertBlocks(metadata: ParquetMetadata, schema: MessageType): Array[ParquetBlockMetadata] = {
    val blocks = metadata.getBlocks().asScala.map { block =>
      ParquetBlockMetadata(block.getRowCount,
        convertColumns(block.getColumns().asScala, schema))
    }
    blocks.toArray
  }

  /**
   * Get task context based on hadoop configuration and partition index.
   * Essentially a shortcut that is used in tests too.
   */
  def taskAttemptContext(conf: Configuration, partitionIndex: Int): TaskAttemptContext = {
    val attemptId = new TaskAttemptID(new TaskID(new JobID(UUID.randomUUID.toString, 0),
      TaskType.MAP, partitionIndex), 0)
    new TaskAttemptContextImpl(conf, attemptId)
  }

  def convertColumns(
      columns: Seq[ColumnChunkMetaData],
      schema: MessageType): Map[String, ParquetColumnMetadata] = {
    val seq = columns.flatMap { column =>
      // process column metadata if column is a part of the schema
      if (schema.containsPath(column.getPath.toArray)) {
        // normalize path as "a.b.c"
        val dotString = column.getPath.toDotString
        val fieldIndex = schema.getFieldIndex(dotString)
        // convert min-max statistics into internal format
        val columnMinMaxStats = convertStatistics(column.getStatistics)
        Some(ParquetColumnMetadata(dotString, column.getValueCount, columnMinMaxStats, None))
      } else {
        None
      }
    }

    seq.map { meta => (meta.fieldName, meta) }.toMap
  }

  def convertStatistics(parquetStatistics: Statistics[_]): ParquetColumnStatistics = {
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

  /**
   * Update each block metadata in array by creating bloom filters. Bloom filter is collected for
   * a file and does not account for file blocks.
   */
  def withBloomFilters(
      indexSchema: MessageType,
      context: TaskAttemptContext,
      blocks: Array[ParquetBlockMetadata],
      status: FileStatus,
      filterDirStatus: FileStatus): Array[ParquetBlockMetadata] = {
    val filters = prepareBloomFilter(indexSchema, blocks)
    // creating split for reading
    val fs = filterDirStatus.getPath.getFileSystem(context.getConfiguration)
    val targetDir =
      filterDirStatus.getPath.suffix(s"${Path.SEPARATOR}${context.getTaskAttemptID.getTaskID}")
    if (!fs.mkdirs(targetDir)) {
      throw new IOException(s"Failed to create target directory $targetDir to store filters")
    }

    // TODO: pass locations from file status, currently it is an empty array
    val split = new FileSplit(status.getPath, 0, status.getLen, Array.empty)
    val parquetSplit = new ParquetInputSplit(split.getPath, split.getStart,
      split.getStart + split.getLength, split.getLength, split.getLocations, null)
    // make requested schema to be available for record reader
    context.getConfiguration.set(ParquetMetastoreSupport.READ_SCHEMA, indexSchema.toString)

    val reader = new ParquetRecordReader[Container](new ParquetIndexReadSupport())
    try {
      reader.initialize(parquetSplit, context)
      // read container and update bloom filters for every column
      while (reader.nextKeyValue) {
        val container = reader.getCurrentValue()
        filters.foreach { case (index, (columnName, bloomFilter)) =>
          if (container.getByIndex(index) != null) {
            bloomFilter.put(container.getByIndex(index))
          }
        }
      }

      // done collecting filter statistics, store them on disk and update stats
      // TODO: currently bloom filter is collected across all blocks, e.g. sequence of blocks will
      // have reference to the same bloom filter, this should be fixed
      val updatedFilters = filters.map { case (index, (columnName, bloomFilter)) =>
        val fileName = s"bloom.$index"
        val filePath = targetDir.suffix(s"${Path.SEPARATOR}$fileName")
        val out = fs.create(filePath, false)
        try {
          bloomFilter.writeTo(out)
        } finally {
          out.close()
        }

        (columnName, Some(ParquetBloomFilter(filePath.toString)))
      }

      blocks.map { block =>
        ParquetBlockMetadata(
          block.rowCount,
          block.indexedColumns.map { case (columnName, metadata) =>
            val filter = updatedFilters.getOrElse(columnName, None)
            (columnName, metadata.withFilter(filter))
          }
        )
      }
    } finally {
      reader.close()
    }
  }

  /**
   * Prepare bloom filter per column, blocks are used to get total number of records for fpp
   * estimation.
   */
  private[parquet] def prepareBloomFilter(
      schema: MessageType,
      blocks: Array[ParquetBlockMetadata]): Map[Int, (String, BloomFilter)] = {
    // all columns should have the same index across blocks
    if (blocks.nonEmpty) {
      val numRows = blocks.map(_.rowCount).sum
      val columns = blocks.head.indexedColumns
      schema.getFields.asScala.flatMap { field =>
        val index = schema.getFieldIndex(field.getName)
        if (columns.contains(field.getName)) {
          // create bloom filter with fpp of 3%
          Some((index, (field.getName, BloomFilter.create(numRows, 0.03))))
        } else {
          None
        }
      }.toMap
    } else {
      Map.empty
    }
  }
}
