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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import com.github.lightcopy.util.SerializableFileStatus

////////////////////////////////////////////////////////////////
// == Parquet metadata for row group and column ==
////////////////////////////////////////////////////////////////

/** Information about column statistics, including optional filter */
case class ParquetColumnMetadata(
    fieldName: String,
    valueCount: Long,
    stats: ColumnStatistics,
    filter: Option[ColumnFilterStatistics]) {

  /** Update current column metadata with new filter */
  def withFilter(newFilter: Option[ColumnFilterStatistics]): ParquetColumnMetadata = {
    // do check on option value
    val updatedFilter = newFilter match {
      case Some(null) | None => None
      case valid @ Some(_) => valid
    }
    ParquetColumnMetadata(fieldName, valueCount, stats, updatedFilter)
  }
}

/** Block metadata stores information about indexed columns */
case class ParquetBlockMetadata(
    rowCount: Long,
    indexedColumns: Map[String, ParquetColumnMetadata])

/**
 * Extended Parquet file status to preserve schema and file status. Also allows to set Spark SQL
 * metadata from Parquet schema as `sqlSchema`.
 */
case class ParquetFileStatus(
    status: SerializableFileStatus,
    fileSchema: String,
    blocks: Array[ParquetBlockMetadata],
    sqlSchema: Option[String] = None) {
  def numRows(): Long = {
    if (blocks.isEmpty) 0 else blocks.map { _.rowCount }.sum
  }
}

/** [[ParquetPartition]] is extended version of `Partition` in Spark */
case class ParquetPartition(
    values: InternalRow,
    files: Seq[ParquetFileStatus])

/**
 * [[ParquetIndexMetadata]] stores full information about Parquet table, including file path,
 * partitions and indexed columns, and used to reconstruct file index.
 */
case class ParquetIndexMetadata(
    tablePath: String,
    dataSchema: StructType,
    indexSchema: StructType,
    partitionSpec: PartitionSpec,
    partitions: Seq[ParquetPartition])
