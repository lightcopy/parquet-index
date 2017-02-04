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

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.DataSourceScanExec.{INPUT_PATHS, PUSHED_FILTERS}
import org.apache.spark.sql.execution.SparkPlan

object IndexSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters,
      l @ LogicalRelation(fsRelation: HadoopFsRelation, _, table))
      if fsRelation.location.isInstanceOf[MetastoreIndexCatalog] =>

      val sparkSession = fsRelation.sparkSession
      val catalog = fsRelation.location.asInstanceOf[MetastoreIndexCatalog]
      val resolver = sparkSession.sessionState.analyzer.resolver

      // Split filters on partitioning filters, index filters and data filters
      val filterSet = ExpressionSet(filters)

      // The attribute name of predicate could be different than the one in schema in case of
      // case insensitive, we should change them to match the one in schema, so we do not need to
      // worry about case sensitivity anymore.
      val normalizedFilters = filters.map { e =>
        e transform {
          case a: AttributeReference =>
            a.withName(l.output.find(_.semanticEquals(a)).get.name)
        }
      }

      // Resolve partitioning columns and extract filters
      val partitionColumns = l.resolve(fsRelation.partitionSchema, resolver)
      val partitionSet = AttributeSet(partitionColumns)
      val partitionKeyFilters = ExpressionSet(
        normalizedFilters.filter(_.references.subsetOf(partitionSet)))
      logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")

      // Resolve index schema and extract index filters
      val indexColumns = l.resolve(catalog.indexSchema, resolver)
      val indexSet = AttributeSet(indexColumns)
      val indexFilters = normalizedFilters.filter(_.references.subsetOf(indexSet)).
        flatMap(DataSourceStrategy.translateFilter)
      logInfo(s"Applying index filters: ${indexFilters.mkString(",")}")

      // select relevant partitions based on index filters and partition keys
      val selectedPartitions =
        catalog.listFilesWithIndexSupport(partitionKeyFilters.toSeq, indexFilters)

      val dataColumns = l.resolve(fsRelation.dataSchema, resolver)
      // Partition keys are not available in the statistics of the files.
      val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

      // Predicates with both partition keys and attributes need to be evaluated after the scan.
      val afterScanFilters = filterSet -- partitionKeyFilters
      logInfo(s"Post-Scan filters: ${afterScanFilters.mkString(",")}")

      val filterAttributes = AttributeSet(afterScanFilters)
      val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
      val requiredAttributes = AttributeSet(requiredExpressions)

      val readDataColumns = dataColumns.
        filter(requiredAttributes.contains).
        filterNot(partitionColumns.contains)
      val outputSchema = readDataColumns.toStructType
      logInfo(s"Output data schema: ${outputSchema.simpleString(5)}")

      val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
      logInfo(s"Pushed filters: ${pushedDownFilters.mkString(",")}")

      val readFile = fsRelation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = sparkSession,
        dataSchema = fsRelation.dataSchema,
        partitionSchema = fsRelation.partitionSchema,
        requiredSchema = outputSchema,
        filters = pushedDownFilters,
        options = fsRelation.options,
        hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(fsRelation.options))

      val plannedPartitions = fsRelation.bucketSpec match {
        case Some(bucketing) if sparkSession.sessionState.conf.bucketingEnabled =>
          logInfo(s"Planning with ${bucketing.numBuckets} buckets")
          val bucketed =
            selectedPartitions.flatMap { p =>
              p.files.map { f =>
                val hosts = getBlockHosts(getBlockLocations(f), 0, f.getLen)
                PartitionedFile(p.values, f.getPath.toUri.toString, 0, f.getLen, hosts)
              }
            }.groupBy { f =>
              BucketingUtils.
                getBucketId(new Path(f.filePath).getName).
                getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
            }

          (0 until bucketing.numBuckets).map { bucketId =>
            FilePartition(bucketId, bucketed.getOrElse(bucketId, Nil))
          }

        case _ =>
          val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
          val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
          val defaultParallelism = sparkSession.sparkContext.defaultParallelism
          val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
          val bytesPerCore = totalBytes / defaultParallelism
          val maxSplitBytes = Math.min(defaultMaxSplitBytes,
            Math.max(openCostInBytes, bytesPerCore))
          logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
            s"open cost is considered as scanning $openCostInBytes bytes.")

          val splitFiles = selectedPartitions.flatMap { partition =>
            partition.files.flatMap { file =>
              val blockLocations = getBlockLocations(file)
              val canBeSplit = fsRelation.fileFormat.isSplitable(sparkSession, fsRelation.options,
                file.getPath)
              if (canBeSplit) {
                (0L until file.getLen by maxSplitBytes).map { offset =>
                  val remaining = file.getLen - offset
                  val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
                  val hosts = getBlockHosts(blockLocations, offset, size)
                  PartitionedFile(
                    partition.values, file.getPath.toUri.toString, offset, size, hosts)
                }
              } else {
                val hosts = getBlockHosts(blockLocations, 0, file.getLen)
                Seq(PartitionedFile(
                  partition.values, file.getPath.toUri.toString, 0, file.getLen, hosts))
              }
            }
          }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

          val partitions = new ArrayBuffer[FilePartition]
          val currentFiles = new ArrayBuffer[PartitionedFile]
          var currentSize = 0L

          /** Add the given file to the current partition. */
          def addFile(file: PartitionedFile): Unit = {
            currentSize += file.length + openCostInBytes
            currentFiles.append(file)
          }

          /** Close the current partition and move to the next. */
          def closePartition(): Unit = {
            if (currentFiles.nonEmpty) {
              val newPartition =
                FilePartition(
                  partitions.size,
                  currentFiles.toArray.toSeq) // Copy to a new Array.
              partitions.append(newPartition)
            }
            currentFiles.clear()
            currentSize = 0
          }

          // Assign files to partitions using "First Fit Decreasing" (FFD)
          splitFiles.foreach { file =>
            if (currentSize + file.length > maxSplitBytes) {
              closePartition()
            }
            addFile(file)
          }
          closePartition()
          partitions
      }

      val meta = Map(
        "Format" -> fsRelation.fileFormat.toString,
        "ReadSchema" -> outputSchema.simpleString,
        PUSHED_FILTERS -> pushedDownFilters.mkString("[", ", ", "]"),
        INPUT_PATHS -> catalog.paths.mkString(", "))

      val scan = DataSourceScanExec.create(readDataColumns ++ partitionColumns,
        new FileScanRDD(sparkSession, readFile, plannedPartitions), fsRelation, meta, table)

      val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
      val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
      val withProjections = if (projects == withFilter.output) {
        withFilter
      } else {
        execution.ProjectExec(projects, withFilter)
      }

      withProjections :: Nil
    case _ => Nil
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  private def getBlockHosts(
      blockLocations: Array[BlockLocation], offset: Long, length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if offset <= b.getOffset && offset + length < b.getLength =>
        b.getHosts -> (offset + length - b.getOffset).min(length)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }
}
