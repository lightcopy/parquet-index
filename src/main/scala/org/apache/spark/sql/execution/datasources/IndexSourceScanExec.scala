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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

case class IndexSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    outputSchema: StructType,
    partitionFilters: Seq[Expression],
    indexFilters: Seq[Filter],
    dataFilters: Seq[Filter])
  extends DataSourceScanExec {

  override val metastoreTableIdentifier: Option[TableIdentifier] = None
  override val rdd: RDD[InternalRow] = null

  override protected def doExecute(): RDD[InternalRow] = null
}
