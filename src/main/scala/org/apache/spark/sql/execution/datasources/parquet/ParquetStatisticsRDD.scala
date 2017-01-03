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

import java.util.UUID

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputSplit, ParquetRecordReader}
import org.apache.parquet.schema.MessageType

import org.apache.spark.{SparkContext, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BloomFilterStatistics, ColumnFilterStatistics, ColumnStatistics}
import org.apache.spark.sql.types.StructType

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
    val fs = FileSystem.get(configuration)
    val partition = split.asInstanceOf[ParquetStatisticsPartition]
    // convert schema of struct type into Parquet schema
    val indexSchema: MessageType = new ParquetSchemaConverter().convert(schema)
    logInfo(s"""
      | == Indexed schema ==
      | ${schema.simpleString}
      | == Converted Parquet schema ==
      | $indexSchema
      """.stripMargin)
    // resolve filter directory as root for all column filter statistics. If path is defined,
    // then use it to store serialized filter data, otherwise it is assumed that filter
    // statistics are disabled.
    // we reconstruct filter directory, and create it, if does not exist, this is different from
    // previous behaviour, where it expected valid path
    val filterDirectory = Option(configuration.get(ParquetMetastoreSupport.FILTER_DIR)).
      map { path =>
        val partitionFileName = String.format("%05d", partition.index.asInstanceOf[Object])
        val filterPath = new Path(path).suffix(s"${Path.SEPARATOR}part-f-$partitionFileName")
        fs.mkdirs(filterPath)
        fs.getFileStatus(filterPath)
      }
    log.info(s"Filter directory enabled: ${filterDirectory.isDefined}")
    // files iterator
    val iter = partition.iterator

    new Iterator[ParquetFileStatus]() {
      override def hasNext: Boolean = {
        iter.hasNext
      }

      override def next(): ParquetFileStatus = {
        val serdeStatus = iter.next
        val parquetStatus = SerializableFileStatus.toFileStatus(serdeStatus)
        logDebug(s"Reading file ${parquetStatus.getPath}")
        val attemptContext = ParquetStatisticsRDD.taskAttemptContext(configuration)
        // read metadata from the file footer
        val metadata = ParquetFileReader.readFooter(configuration, parquetStatus, NO_FILTER)
        val fileSchema = metadata.getFileMetaData.getSchema
        // check that requested schema is part of the file schema
        fileSchema.checkContains(indexSchema)
        // extract unique map of top level columns
        val topLevelColumns = ParquetSchemaUtils.topLevelUniqueColumns(indexSchema).toMap
        // blocks map one-to-one to the Parquet block metadata, filters and statistics are
        // maintained per block
        val blocks = metadata.getBlocks.asScala.zipWithIndex.map { case (block, blockIndex) =>
          // prepare statistics map
          val statisticsMap = ParquetStatisticsRDD.schemaBasedStatistics(schema)
          // prepare statistics-filter map, this adds column filter to each statistics, if available
          val statFilterMap = statisticsMap.map { case (columnName, statistics) =>
            val columnIndex = topLevelColumns.getOrElse(columnName,
              sys.error(s"Failed to look up $columnName"))
            filterDirectory match {
              case Some(filterStatus) =>
                val columnFilter = BloomFilterStatistics(block.getRowCount)
                val filename =
                  s"${attemptContext.getTaskAttemptID.getTaskID}-$blockIndex-$columnName-filter"
                columnFilter.setPath(filterStatus.getPath.suffix(s"${Path.SEPARATOR}$filename"))
                (columnIndex, (columnName, statistics, Some(columnFilter)))
              case None =>
                (columnIndex, (columnName, statistics, None))
            }
          }.toMap

          (block.getRowCount, statFilterMap)
        }

        // blocks can be empty for empty Parquet file, in this case we just skip the reader step
        val blockMetadata: Array[ParquetBlockMetadata] = if (blocks.nonEmpty) {
          // create reader and run statistics using blocks
          // TODO: pass locations from file status, currently it is an empty array
          val split = new FileSplit(parquetStatus.getPath, 0, parquetStatus.getLen, Array.empty)
          val parquetSplit = new ParquetInputSplit(split.getPath, split.getStart,
            split.getStart + split.getLength, split.getLength, split.getLocations, null)
          // make index schema to be available for record reader
          attemptContext.getConfiguration.set(ParquetMetastoreSupport.READ_SCHEMA,
            indexSchema.toString)

          val reader = new ParquetRecordReader[Container](new ParquetIndexReadSupport())
          try {
            reader.initialize(parquetSplit, attemptContext)
            // current block index, when row count > record count we select next block
            var currentBlockIndex = -1
            // total record count to select next block
            var rowCount: Long = 0

            while (reader.nextKeyValue && currentBlockIndex < blocks.length) {
              val container = reader.getCurrentValue()
              if (rowCount == 0) {
                // select next block
                currentBlockIndex += 1
                rowCount = blocks(currentBlockIndex)._1
              }
              val statFilterMap = blocks(currentBlockIndex)._2
              // update statistics and column filter with current value
              statFilterMap.foreach { case (columnIndex, (columnName, statistics, columnFilter)) =>
                if (container.getByIndex(columnIndex) != null) {
                  statistics.updateMinMax(container.getByIndex(columnIndex))
                  if (columnFilter.isDefined) {
                    columnFilter.get.update(container.getByIndex(columnIndex))
                  }
                } else {
                  statistics.incrementNumNulls()
                }
              }
              rowCount -= 1
            }

            if (reader.nextKeyValue || currentBlockIndex < blocks.length - 1 || rowCount != 0) {
              throw new IllegalStateException()
            }

            // write filter data on disk (if filter supports it) after all write checks
            blocks.foreach { case (rowCount, map) =>
              map.foreach { case (_, (_, _, filter)) =>
                if (filter.isDefined) {
                  filter.get.writeData(fs, configuration)
                }
              }
            }
          } finally {
            reader.close()
          }
          ParquetStatisticsRDD.convertBlocks(blocks)
        } else {
          Array.empty
        }
        // currently global Parquet schema is merged and inferred on a driver, which is suboptimal
        // since we can reduce schema during metadata collection.
        // TODO: Partially merge schema during each task
        ParquetFileStatus(serdeStatus, fileSchema.toString, blockMetadata)
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
   * Get task context based on hadoop configuration.
   * Essentially a shortcut that is used in tests too.
   */
  def taskAttemptContext(conf: Configuration): TaskAttemptContext = {
    val attemptId = new TaskAttemptID(new TaskID(new JobID(UUID.randomUUID.toString, 0),
      TaskType.MAP, 0), 0)
    new TaskAttemptContextImpl(conf, attemptId)
  }

  /**
   * Parse schema into map of column names and column statistics.
   * Method does not verify if schema has duplicate fields.
   */
  def schemaBasedStatistics(schema: StructType): Map[String, ColumnStatistics] = {
    schema.fields.map { field =>
      (field.name, ColumnStatistics.getStatisticsForType(field.dataType))
    }.toMap
  }

  /**
   * Convert blocks into array of [[ParquetBlockMetadata]].
   */
  def convertBlocks(
      blocks: Seq[(Long, Map[Int, (String, ColumnStatistics,
        Option[ColumnFilterStatistics])])]): Array[ParquetBlockMetadata] = {
    blocks.map { case (rowCount, statFilterMap) =>
      val indexedColumns = statFilterMap.map { case (index, (columnName, stats, filter)) =>
        // for value count we include all records in block metadata, including null values
        (columnName, ParquetColumnMetadata(columnName, rowCount, stats, filter))
      }
      ParquetBlockMetadata(rowCount, indexedColumns)
    }.toArray
  }
}
