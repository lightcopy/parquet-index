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

package com.github.lightcopy.index.parquet

import org.apache.spark.sql.Column

import com.github.lightcopy.{Catalog, IndexSpec}
import com.github.lightcopy.index.{Index, IndexSource, Metadata}

class ParquetSource extends IndexSource {
  override def loadIndex(
      catalog: Catalog,
      name: String,
      root: String,
      metadata: Option[Metadata]): Index = {
    null
  }

  override def createIndex(catalog: Catalog, indexSpec: IndexSpec, columns: Seq[Column]): Index = {
    null
  }
}
