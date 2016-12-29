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

import org.apache.spark.sql.sources._

private[parquet] case class ParquetIndexFilters(
    conf: Configuration,
    blocks: Array[ParquetBlockMetadata]) {
  /**
   * Transform filter with provided statistics and filter.
   */
  private def transformFilter(attribute: String)
      (func: (ParquetColumnStatistics, Option[ParquetColumnFilter]) => Filter): Filter = {
    val references = blocks.map { block =>
      block.indexedColumns.get(attribute) match {
        case Some(columnMetadata) =>
          val stats = columnMetadata.stats
          val filter = columnMetadata.filter
          func(stats, filter)
        case None =>
          // Attribute is not indexed, return trivial filter to scan file
          Trivial(true)
      }
    }
    // all filters must be resolved at this point
    foldFilter(references.reduceLeft(Or))
  }

  /**
   * Recursively fold provided index filters to trivial,
   * blocks are always non-empty.
   */
  def foldFilter(filter: Filter): Filter = {
    filter match {
      case EqualTo(attribute: String, value: Any) =>
        transformFilter(attribute) { case (stats, filter) =>
          val foundInStats = stats.contains(value)
          if (foundInStats && filter.isDefined) {
            val columnFilter = filter.get
            columnFilter.init(conf)
            Trivial(columnFilter.mightContain(value))
          } else {
            Trivial(foundInStats)
          }
        }
      case In(attribute: String, values: Array[Any]) =>
        transformFilter(attribute) { case (stats, filter) =>
          val foundInStats = values.exists(stats.contains)
          if (foundInStats && filter.isDefined) {
            val columnFilter = filter.get
            columnFilter.init(conf)
            Trivial(values.exists(columnFilter.mightContain))
          } else {
            Trivial(foundInStats)
          }
        }
      case IsNull(attribute: String) =>
        transformFilter(attribute) { case (stats, _) =>
          Trivial(stats.hasNull)
        }
      case GreaterThan(attribute: String, value: Any) =>
        transformFilter(attribute) { case (stats, _) =>
          // we check if value is greateer than max - definitely not in range, or value is equal to
          // max, but since we want everything greater than that, this will still yield false
          Trivial(!(stats.isGreaterThanMax(value) || stats.isEqualToMax(value)))
        }
      case GreaterThanOrEqual(attribute: String, value: Any) =>
        transformFilter(attribute) { case (stats, _) =>
          // equaity value == max is valid and is in range
          Trivial(!stats.isGreaterThanMax(value))
        }
      case LessThan(attribute: String, value: Any) =>
        transformFilter(attribute) { case (stats, _) =>
          // similar to GreaterThan, we check if value is less than min or is equal to min,
          // otherwise is considered in range
          Trivial(!(stats.isLessThanMin(value) || stats.isEqualToMin(value)))
        }
      case LessThanOrEqual(attribute: String, value: Any) =>
        transformFilter(attribute) { case (stats, _) =>
          // if value is equal to min, we still have to scan file
          Trivial(!stats.isLessThanMin(value))
        }
      case And(left: Filter, right: Filter) =>
        And(foldFilter(left), foldFilter(right)) match {
          case And(Trivial(false), _) => Trivial(false)
          case And(_, Trivial(false)) => Trivial(false)
          case And(Trivial(true), right) => right
          case And(left, Trivial(true)) => left
          case other => other
        }
      case Or(left: Filter, right: Filter) =>
        Or(foldFilter(left), foldFilter(right)) match {
          case Or(Trivial(false), right) => right
          case Or(left, Trivial(false)) => left
          case Or(Trivial(true), _) => Trivial(true)
          case Or(_, Trivial(true)) => Trivial(true)
          case other => other
        }
      case Not(child: Filter) =>
        Not(foldFilter(child)) match {
          case Not(Trivial(false)) => Trivial(true)
          case Not(Trivial(true)) => Trivial(false)
          case other => other
        }
      case trivial: Trivial =>
        trivial
      case unsupportedFilter =>
        // return 'true' to scan all partitions
        // currently unsupported filters are:
        // - IsNotNull
        // - StringStartsWith
        // - StringEndsWith
        // - StringContains
        // - EqualNullSafe
        Trivial(true)
    }
  }
}
