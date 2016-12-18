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
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}

object IndexSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters,
      l @ LogicalRelation(fsRelation: HadoopFsRelation, _, _))
      if fsRelation.location.getClass == classOf[MetastoreIndexCatalog] =>

      val catalog = fsRelation.location.asInstanceOf[MetastoreIndexCatalog]
      val resolver = fsRelation.sparkSession.sessionState.analyzer.resolver

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

      val outputAttributes = readDataColumns ++ partitionColumns

      val scan = new IndexSourceScanExec(
        fsRelation,
        outputAttributes,
        outputSchema,
        partitionKeyFilters.toSeq,
        indexFilters,
        pushedDownFilters)

      val afterScanFilter = afterScanFilters.toSeq.reduceOption(And)
      val withFilter = afterScanFilter.map(FilterExec(_, scan)).getOrElse(scan)
      val withProjections = if (projects == withFilter.output) {
        withFilter
      } else {
        ProjectExec(projects, withFilter)
      }

      withProjections :: Nil
    case other => Nil
  }
}
