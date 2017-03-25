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

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import com.github.lightcopy.util.SerializableFileStatus

/**
 * Index catalog for Parquet tables.
 * Metastore is used mainly to provide Hadoop file system and/or configuration.
 */
class ParquetIndexCatalog(
    @transient val metastore: Metastore,
    @transient val indexMetadata: ParquetIndexMetadata)
  extends MetastoreIndexCatalog {

  require(indexMetadata != null, "Parquet index metadata is null, serialized data is incorrect")

  override val tablePath: Path = new Path(indexMetadata.tablePath)

  override val partitionSpec: PartitionSpec = indexMetadata.partitionSpec

  override lazy val dataSchema = indexMetadata.dataSchema

  override lazy val indexSchema = indexMetadata.indexSchema

  override def listFilesWithIndexSupport(
      filters: Seq[Expression], indexFilters: Seq[Filter]): Seq[Partition] = {
    // select all parquet file statuses if partition schema is empty
    val allPartitions = indexMetadata.partitions
    val selectedPartitions: Seq[ParquetPartition] = if (partitionSpec.partitionColumns.isEmpty) {
      allPartitions
    } else {
      // here we need to check path for each partition leaf that it is contains partition directory
      // currently check is based on partitions having the same parsed values as directory
      prunePartitions(filters, partitionSpec).flatMap { case PartitionDirectory(values, path) =>
        allPartitions.filter { partition =>
          partition.values == values
        }
      }
    }

    logDebug("Selected files after partition pruning:\n\t" + selectedPartitions.mkString("\n\t"))

    // evaluate index filters
    val filteredPartitions = if (indexFilters.isEmpty) {
      selectedPartitions
    } else {
      val startTime = System.nanoTime
      val indexedPartitions = pruneIndexedPartitions(indexFilters, selectedPartitions)
      val endTime = System.nanoTime()
      def timeMs: Double = (endTime - startTime).toDouble / 1000000
      logInfo(s"Filtered indexed partitions in $timeMs ms")
      indexedPartitions
    }

    logInfo(s"Selected ${filteredPartitions.map(_.files.length).sum} files to scan")
    logDebug("Selected files after index filtering:\n\t" + filteredPartitions.mkString("\n\t"))

    // convert it into sequence of Spark `Partition`s
    filteredPartitions.map { partition =>
      Partition(partition.values, partition.files.map { file =>
        SerializableFileStatus.toFileStatus(file.status)
      })
    }
  }

  override def allFiles(): Seq[FileStatus] = indexMetadata.partitions.flatMap { partition =>
    partition.files.map { parquetFile => SerializableFileStatus.toFileStatus(parquetFile.status) }
  }

  private[parquet] def prunePartitions(
      predicates: Seq[Expression],
      partitionSpec: PartitionSpec): Seq[PartitionDirectory] = {
    val PartitionSpec(partitionColumns, partitions) = partitionSpec
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = InterpretedPredicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      })

      val selected = partitions.filter {
        case PartitionDirectory(values, _) => boundPredicate(values)
      }

      logInfo {
        val total = partitions.length
        val selectedSize = selected.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        s"Selected $selectedSize partitions out of $total, pruned $percentPruned% partitions."
      }

      selected
    } else {
      partitions
    }
  }

  /**
   * Since [[ParquetFileStatus]] can contain multiple blocks we have to resolve all of them and
   * result should be `Or` of all subresults.
   */
  private[parquet] def resolveSupported(
      filter: Filter,
      status: ParquetFileStatus): Filter = {
    // we need file system to resolve column filters
    ParquetIndexFilters(metastore.fs, status.blocks).foldFilter(filter)
  }

  private[parquet] def pruneIndexedPartitions(
      indexFilters: Seq[Filter],
      partitions: Seq[ParquetPartition]): Seq[ParquetPartition] = {
    require(indexFilters.nonEmpty, s"Expected non-empty index filters, got $indexFilters")
    // reduce filters to supported only
    val reducedFilter = indexFilters.reduceLeft(And)

    // use futures to reduce IO cost when reading filter files
    implicit val executorContext = ExecutionContext.global
    partitions.flatMap { partition =>
      val futures = partition.files.map { file =>
        Future[Option[ParquetFileStatus]] {
          resolveSupported(reducedFilter, file) match {
            case Trivial(true) => Some(file)
            case Trivial(false) => None
            case other => sys.error(s"Failed to resolve filter, got $other, expected trivial")
          }
        }(executorContext)
      }

      val filteredStatuses = Await.result(Future.sequence(futures), Duration.Inf).flatten
      if (filteredStatuses.isEmpty) {
        None
      } else {
        Some(ParquetPartition(partition.values, filteredStatuses))
      }
    }
  }
}
