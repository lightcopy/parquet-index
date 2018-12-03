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

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat, _}

case class PartitionMetastoreSupport(fileFormat: FileFormat) extends MetastoreSupport with Logging {
  def this() {
    // For reflection, up to now, we only support parquet
    // maybe we can make it a universal opt
    this(new ParquetFileFormat)
  }

  override def identifier: String = "repartition"

  override def loadIndex(
      metastore: Metastore,
      tableDirectory: FileStatus): MetastoreIndex = {
    new PartitionIndex(metastore, tableDirectory.getPath, fileFormat, Map.empty)
  }

  override def createIndex(
      metastore: Metastore,
      indexDirectory: FileStatus,
      tablePath: FileStatus,
      isAppend: Boolean,
      partitionSpec: PartitionSpec,
      partitions: Seq[PartitionDirectory],
      columns: Seq[Column]): Unit = { }
}


