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

package org.apache.spark.sql.execution.datasources.lightweight

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{LongType, StructType}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class PartitionIndex(
    val metastore: Metastore,
    val tablePath: Path,
    val fileFormat: FileFormat,
    val options: Map[String, String])
  extends MetastoreIndex {

  val catalog = new InMemoryFileIndex(metastore.session, Seq(tablePath), options, None)

  // internal set of index filters that we reset every time when loading relation
  private var internalIndexFilters: Seq[Filter] = Nil

  override lazy val partitionSchema: StructType = catalog.partitionSpec.partitionColumns

  override lazy val dataSchema: StructType = fileFormat.inferSchema(
                                      metastore.session, options, catalog.allFiles()).get

  override lazy val indexSchema: StructType = StructType(
    if (options.get("repartitionedColumn").isDefined) {
      dataSchema.filter(_.name == options("repartitionedColumn"))
    } else {
      Seq.empty
    })


  override def setIndexFilters(filters: Seq[Filter]): Unit = {
    internalIndexFilters = filters
  }

  override def indexFilters: Seq[Filter] = internalIndexFilters

  override def listFilesWithIndexSupport(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      indexFilters: Seq[Filter]): Seq[PartitionDirectory] = {
    val partitionSpec = catalog.partitionSpec

    // use InMemoryFileIndex for pruning partitions
    val selectedPartitions = catalog.listFiles(partitionFilters, dataFilters)

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

    logDebug("Selected files after index filtering:\n\t" + filteredPartitions.mkString("\n\t"))

    // convert it into sequence of Spark `PartitionDirectory`s
    filteredPartitions
  }

  private def pruneIndexedPartitions(
      indexFilters: Seq[Filter],
      partitions: Seq[PartitionDirectory]): Seq[PartitionDirectory] = {
    require(indexFilters.nonEmpty, s"Expected non-empty index filters, got $indexFilters")
    // reduce filters to supported only
    val reducedFilter = indexFilters.reduceLeft(And)

    // use futures to reduce IO cost when reading filter files
    implicit val executorContext: ExecutionContextExecutor = ExecutionContext.global
    partitions.flatMap { partition =>
      val fileSize = partition.files.length
      val futures = partition.files.sortBy(_.getPath.toString).zipWithIndex.map { tp =>
        Future[Option[FileStatus]] {

          resolveSupported(reducedFilter, tp._2, fileSize) match {
            case Trivial(true) => Some(tp._1)
            case Trivial(false) => None
            case other => sys.error(s"Failed to resolve filter, got $other, expected trivial")
          }
        }(executorContext)
      }

      val filteredStatuses = Await.result(Future.sequence(futures), Duration.Inf).flatten

      if (filteredStatuses.isEmpty) {
        None
      } else {
        Some(PartitionDirectory(partition.values, filteredStatuses))
      }
    }
  }

  override lazy val inputFiles: Array[String] = catalog.listFiles(Nil, Nil).flatMap { partition =>
    partition.files.map(_.getPath.toString)
  }.toArray

  override lazy val sizeInBytes: Long = catalog.listFiles(Nil, Nil).flatMap { partition =>
    partition.files.map(_.getLen)
  }.sum

  private def resolveSupported(
      filter: Filter,
      fileSize: Int,
      pos: Int): Filter = {
    PartitionIndexFilters(pos, LongType, fileSize).foldFilter(filter)
  }

}