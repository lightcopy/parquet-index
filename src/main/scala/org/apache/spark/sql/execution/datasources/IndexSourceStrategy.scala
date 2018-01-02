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

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

object IndexSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters,
      l @ LogicalRelation(fsRelation: HadoopFsRelation, _, table))
      if fsRelation.location.isInstanceOf[MetastoreIndex] =>

      val catalog = fsRelation.location.asInstanceOf[MetastoreIndex]
      val resolver = fsRelation.sparkSession.sessionState.analyzer.resolver

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

      val partitionColumns = l.resolve(fsRelation.partitionSchema, resolver)
      val partitionSet = AttributeSet(partitionColumns)
      val partitionKeyFilters =
        ExpressionSet(normalizedFilters.filter(_.references.subsetOf(partitionSet)))
      logDebug(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")

      // Resolve index schema and extract index filters
      // Filters will only be applied for indexed columns that allow predicate to be separately
      // evaluated for those columns, e.g. And(Eq(col1, value1), Eq(col2, value2)), when all
      // columns in Or are indexed.
      // In the case when filter cannot be split, for example, when filter is
      // Or(Eq(col1, value1), Eq(col2, value2)) and index exists only on one of the columns (col1),
      // selected filters will be empty, otherwise Or evaluation would be incomplete.
      val indexColumns = l.resolve(catalog.indexSchema, resolver)
      val indexSet = AttributeSet(indexColumns)
      val indexFilters = normalizedFilters.filter(_.references.subsetOf(indexSet)).
        flatMap(DataSourceStrategy.translateFilter)
      // Hack to propagate filters into FileSourceScanExec instead of copying a lot of code to
      // add index filters, catalog will reset filters every time this method is called
      catalog.setIndexFilters(indexFilters)
      logInfo(s"Applying index filters: ${indexFilters.mkString(",")}")
      if (indexFilters.isEmpty) {
        logWarning(s"Cannot extract predicate for indexed columns $indexColumns from normalized " +
          s"filters $normalizedFilters, predicate will have to be evaluated as part of scan. " +
          "Try to index all columns that appear in normalized filters and/or update predicate " +
          "to use indexed columns in combination with other filters using 'And'; when using " +
          "'Or' make sure that both branches contain indexed columns")
      }

      val dataColumns = l.resolve(fsRelation.dataSchema, resolver)

      // Partition keys are not available in the statistics of the files.
      val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

      // Predicates with both partition keys and attributes need to be evaluated after the scan.
      val afterScanFilters = filterSet -- partitionKeyFilters.filter(_.references.nonEmpty)
      logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

      val filterAttributes = AttributeSet(afterScanFilters)
      val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
      val requiredAttributes = AttributeSet(requiredExpressions)

      val readDataColumns =
        dataColumns
          .filter(requiredAttributes.contains)
          .filterNot(partitionColumns.contains)
      val outputSchema = readDataColumns.toStructType
      logInfo(s"Output Data Schema: ${outputSchema.simpleString(5)}")

      val outputAttributes = readDataColumns ++ partitionColumns

      val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)

      val scan =
        FileSourceScanExec(
          fsRelation,
          outputAttributes,
          outputSchema,
          partitionKeyFilters.toSeq,
          pushedDownFilters,
          table.map(_.identifier))

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
}
