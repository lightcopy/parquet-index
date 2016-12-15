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

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageTypeParser

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

import com.github.lightcopy.util.IOUtils

case class ParquetIndexFileFormat() extends MetastoreSupport {
  override def identifier: String = "parquet"

  override def createIndex(
      metastore: Metastore,
      indexDirectory: FileStatus,
      isAppend: Boolean,
      partitionSpec: PartitionSpec,
      partitions: Seq[Partition],
      columns: Seq[Column]): Unit = {
    require(partitions.nonEmpty, "Empty partitions provided")
    if (isAppend) {
      throw new UnsupportedOperationException(
        s"${getClass.getSimpleName} does not support append to existing index")
    }

    val columnNames = columns.map(_.toString)
    // make sure that columns are not part of partition columns
    val fieldNames = partitionSpec.partitionColumns.fieldNames.toSet

    for (name <- columnNames) {
      if (fieldNames.contains(name)) {
        throw new IllegalArgumentException(s"Found column $name in partitioning schema. " +
          "Currently indexing of partitioning columns is not supported")
      }
    }

    val partitionSchema = partitionSpec.partitionColumns
    // prepare index schema by fetching message type from random file
    val indexSchema = inferIndexSchema(metastore, partitions, columnNames)
    if (indexSchema.isEmpty) {
      throw new UnsupportedOperationException("Index schema must have at least one column, " +
        s"found $indexSchema, make sure that specified columns are part of table schema")
    }

    // Parquet file statuses
    val files = partitions.flatMap { partition =>
      val partSchema = PartitionColumn.fromInternalRow(partition.values, partitionSchema)
      partition.files.map { status =>
        ParquetFileStatus(status.getPath.toString, partSchema)
      }
    }

    val sc = metastore.session.sparkContext
    val hadoopConf = metastore.session.sessionState.newHadoopConf()
    hadoopConf.set(ParquetIndexFileFormat.BLOOM_FILTER_ENABLED,
      metastore.session.conf.get(ParquetIndexFileFormat.BLOOM_FILTER_ENABLED, "false"))
    hadoopConf.set(ParquetIndexFileFormat.BLOOM_FILTER_DIR, indexDirectory.getPath.toString)

    val numPartitions = Math.min(sc.defaultParallelism * 2,
      metastore.session.conf.get("spark.sql.shuffle.partitions").toInt)
    val rdd = new ParquetStatisticsRDD(sc, hadoopConf, indexSchema, files, numPartitions)
    val statistics = rdd.collect

    val table = ParquetTable(
      tablePath = "",
      tableSchema = ParquetIndexFileFormat.inferSchema(metastore.session, statistics),
      indexSchema = indexSchema,
      partitionSchema = Some(partitionSchema),
      statistics = statistics)

    // write table to disk, create path with table metadata
    val metadataDir =
      indexDirectory.getPath.suffix(s"${Path.SEPARATOR}${ParquetIndexFileFormat.TABLE_METADATA}")
    IOUtils.writeContent(metastore.fs, metadataDir, table.toJSON)
  }

  /** Infer schema by requesting provided set of columns */
  private def inferIndexSchema(
      metastore: Metastore, partitions: Seq[Partition], columns: Seq[String]): StructType = {
    val conf = metastore.session.sparkContext.hadoopConfiguration
    val status = partitions.head.files.head

    // read footer and extract schema into struct type
    val footer = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf,
      Arrays.asList(status), false).get(0)
    val schema = footer.getParquetMetadata.getFileMetaData.getSchema
    val fileStruct = new ParquetSchemaConverter().convert(schema)

    // fetch specified columns
    val fields = fileStruct.filter { field => columns.contains(field.name) }
    StructType(fields)
  }
}

object ParquetIndexFileFormat {
  // internal option to set schema for Parquet reader
  val READ_SCHEMA = "spark.sql.index.parquet.read.schema"
  // public option to enable/disable bloom filters
  val BLOOM_FILTER_ENABLED = "spark.sql.index.parquet.bloom.enabled"
  // internal option to specify bloom filters directory
  val BLOOM_FILTER_DIR = "spark.sql.index.parquet.bloom.dir"
  // metadata name
  val TABLE_METADATA = "_table_metadata"

  def inferSchema(sparkSession: SparkSession, statistics: Array[ParquetStatistics]): StructType = {
    val assumeBinaryIsString = sparkSession.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sparkSession.sessionState.conf.isParquetINT96AsTimestamp
    val writeLegacyParquetFormat = sparkSession.sessionState.conf.writeLegacyParquetFormat
    val converter = new ParquetSchemaConverter(
      assumeBinaryIsString = assumeBinaryIsString,
      assumeInt96IsTimestamp = assumeInt96IsTimestamp,
      writeLegacyParquetFormat = writeLegacyParquetFormat)

    if (statistics.isEmpty) {
      StructType(Seq.empty)
    } else {
      val schema = converter.convert(MessageTypeParser.parseMessageType(statistics.head.fileSchema))
      statistics.tail.foreach { stats =>
        schema.merge(converter.convert(MessageTypeParser.parseMessageType(stats.fileSchema)))
      }
      schema
    }
  }
}
