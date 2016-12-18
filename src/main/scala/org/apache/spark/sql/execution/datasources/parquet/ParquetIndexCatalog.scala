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

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class ParquetIndexCatalog(table: ParquetTable) extends MetastoreIndexCatalog {
  private val statistics = table.statistics
  private val partitionSchema = table.partitionSchema.getOrElse(StructType(Nil))

  override lazy val paths: Seq[Path] = new Path(table.tablePath) :: Nil

  override lazy val partitionSpec: PartitionSpec = {
    // TODO: parse partition directories
    PartitionSpec(partitionSchema, Seq.empty[PartitionDirectory])
  }

  override lazy val dataSchema = table.tableSchema

  override lazy val indexSchema = table.indexSchema

  override def listFiles(filters: Seq[Expression]): Seq[Partition] = {
    listFilesWithIndexSupport(filters, Seq.empty)
  }

  override def listFilesWithIndexSupport(
      filters: Seq[Expression], indexFilters: Seq[Filter]): Seq[Partition] = Nil

  override def allFiles(): Seq[FileStatus] = Nil

  override def refresh(): Unit = { }
}
