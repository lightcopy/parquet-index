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

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.Column
import org.apache.spark.sql.execution.datasources._

case class ParquetIndexFileFormat() extends MetastoreSupport {
  override def identifier: String = "parquet"

  override def createIndex(
      metastore: Metastore,
      indexDirectory: FileStatus,
      isAppend: Boolean,
      partitionSpec: PartitionSpec,
      files: Seq[Partition],
      columns: Seq[Column]): Unit = {
    if (isAppend) {
      throw new UnsupportedOperationException(s"${getClass.getSimpleName} does not support append")
    }
  }
}

object ParquetIndexFileFormat {
  val PARQUET_INDEX_READ_SCHEMA = "parquet.index.read.schema"
  val PARQUET_INDEX_BLOOM_FILTER_ENABLED = "parquet.index.bloom.enabled"
  val PARQUET_INDEX_BLOOM_FILTER_DIR = "parquet.index.bloom.dir"
}
