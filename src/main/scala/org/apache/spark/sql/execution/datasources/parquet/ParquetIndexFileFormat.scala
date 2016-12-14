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

import org.apache.hadoop.fs.FileStatus

import org.apache.parquet.hadoop.ParquetFileReader

import org.apache.spark.sql.Column
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

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

    // prepare index schema by fetching message type from random file
    val indexSchema = inferIndexSchema(metastore, partitions, columnNames)
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
  val PARQUET_INDEX_READ_SCHEMA = "spark.sql.index.parquet.read.schema"
  // public option to enable/disable bloom filters
  val PARQUET_INDEX_BLOOM_FILTER_ENABLED = "spark.sql.index.parquet.bloom.enabled"
  // internal option to specify bloom filters directory
  val PARQUET_INDEX_BLOOM_FILTER_DIR = "spark.sql.index.parquet.bloom.dir"
}
