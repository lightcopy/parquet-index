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
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.FileSourceScanExec

/** Catalog table info that is used to reconstruct data source */
sealed case class CatalogTableInfo(
    format: String,
    inputPath: String,
    metadata: Map[String, String])

/** Source for catalog tables */
case class CatalogTableSource(
    val metastore: Metastore,
    val tableName: String,
    val options: Map[String, String],
    val mode: SaveMode = SaveMode.ErrorIfExists) extends Logging {
  // metadata keys to extract
  val FORMAT = "Format"
  val INPUT_PATHS = "InputPaths"
  // parse table identifier and build logical plan
  val tableIdent = metastore.session.sessionState.sqlParser.parseTableIdentifier(tableName)
  val plan = metastore.session.sessionState.catalog.lookupRelation(tableIdent)
  val info = executeSourcePlan(plan)
  logInfo(s"Catalog table info $info")

  private def executeSourcePlan(plan: LogicalPlan): CatalogTableInfo = {
    val qe = metastore.session.sessionState.executePlan(plan)
    qe.assertAnalyzed
    qe.sparkPlan match {
      case scanExec: FileSourceScanExec if scanExec.metastoreTableIdentifier.isDefined =>
        // format describes subclass of FileFormat, and reference is slightly different from
        // datasource API, also we expect only single path/directory
        require(scanExec.metadata.contains(FORMAT), s"$FORMAT for $scanExec")
        require(scanExec.relation.location.rootPaths.length == 1, s"Input paths for $scanExec")

        val format = scanExec.metadata(FORMAT)
        val inputPath = scanExec.relation.location.rootPaths.head.toString
        val extendedOptions = options + ("path" -> inputPath)
        CatalogTableInfo(format, inputPath, extendedOptions)
      case other =>
        throw new UnsupportedOperationException(s"$other")
    }
  }

  /** Convert table source into indexed datasource */
  def asDataSource: IndexedDataSource = {
    IndexedDataSource(
      metastore = metastore,
      className = info.format,
      mode = mode,
      options = info.metadata,
      catalogTable = Some(info))
  }
}
