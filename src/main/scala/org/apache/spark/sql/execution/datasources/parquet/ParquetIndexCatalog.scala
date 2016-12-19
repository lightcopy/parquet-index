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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import com.github.lightcopy.util.SerializableFileStatus

/**
 * Index catalog for Parquet tables.
 * Metastore is used mainly to provide Hadoop configuration.
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

    // TODO: Make it trace after testing
    logTrace("Selected files after partition pruning:\n\t" + selectedPartitions.mkString("\n\t"))

    // evaluate index filters
    val filteredPartitions = if (indexFilters.isEmpty) {
      selectedPartitions
    } else {
      pruneIndexedPartitions(indexFilters, selectedPartitions)
    }

    // TODO: Make it trace after testing
    logInfo("Selected files after index filtering:\n\t" + filteredPartitions.mkString("\n\t"))

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

  private def prunePartitions(
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
  private def resolveSupported(
      filter: Filter,
      status: ParquetFileStatus): Filter = {
    // we need configuration to resolve column filters
    val conf = metastore.session.sessionState.newHadoopConf()
    filter match {
      case eq @ EqualTo(attribute: String, value: Any) =>
        val references = status.blocks.map { block =>
          // find relevant column and resolve based on statistics
          // if column metadata is not found, return 'true' to scan everything
          block.indexedColumns.get(attribute) match {
            case Some(columnMetadata) =>
              val stats = columnMetadata.stats
              Trivial(stats.contains(value)) match {
                case isin @ Trivial(true) =>
                  // check if there is a filter available
                  if (columnMetadata.filter.isDefined) {
                    val columnFilter = columnMetadata.filter.get
                    columnFilter.init(conf)
                    Trivial(columnFilter.mightContain(value))
                  } else {
                    isin
                  }
                case not @ Trivial(false) => not
              }
            case None => Trivial(true)
          }
        }
        // all filters must be resolved at this point
        references.reduceLeft(Or)
      case And(left: Filter, right: Filter) =>
        And(resolveSupported(left, status), resolveSupported(right, status)) match {
          case And(Trivial(false), _) => Trivial(false)
          case And(_, Trivial(false)) => Trivial(false)
          case And(Trivial(true), right) => right
          case And(left, Trivial(true)) => left
          case other => other
        }
      case Or(left: Filter, right: Filter) =>
        Or(resolveSupported(left, status), resolveSupported(right, status)) match {
          case Or(Trivial(false), right) => right
          case Or(left, Trivial(false)) => left
          case Or(Trivial(true), _) => Trivial(true)
          case Or(_, Trivial(true)) => Trivial(true)
          case other => other
        }
      case unsupportedFilter =>
        // return 'true' to scan all partitions
        Trivial(true)
    }
  }

  private def pruneIndexedPartitions(
      indexFilters: Seq[Filter],
      partitions: Seq[ParquetPartition]): Seq[ParquetPartition] = {
    // reduce filters to supported only
    val reducedFilter = indexFilters.reduceLeft(And)
    partitions.flatMap { partition =>
      val filteredStatuses = partition.files.filter { file =>
        resolveSupported(reducedFilter, file) match {
          case Trivial(true) => true
          case Trivial(false) => false
          case other => sys.error(s"Failed to resolve filter, got $other, expected trivial")
        }
      }

      if (filteredStatuses.isEmpty) {
        None
      } else {
        Some(ParquetPartition(partition.values, filteredStatuses))
      }
    }
  }
}
