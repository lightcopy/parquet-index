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

package com.github.lightcopy.index.simple

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import com.github.lightcopy.{Catalog, IndexSpec}
import com.github.lightcopy.index.{Index, IndexSource, Metadata}

/** Simple index for testing */
class SimpleIndex(withCatalog: Catalog) extends Index {
  override def getIndexIdentifier(): String = "simple"
  override def getMetadata(): Metadata = Metadata("simple", None,
    StructType(StructField("a", LongType) :: Nil), Map.empty)
  override def catalog: Catalog = withCatalog
  override def containsSpec(spec: IndexSpec): Boolean = true
  override def append(spec: IndexSpec, columns: Seq[Column]): Unit = { }
  // No DataFrame support
  override def search(condition: Column): DataFrame = null
  override def delete(): Unit = { }
}

/** Simple source for testing */
class SimpleSource extends IndexSource {
  override def loadIndex(catalog: Catalog, metadata: Metadata): Index = {
    new SimpleIndex(catalog)
  }

  override def createIndex(catalog: Catalog, spec: IndexSpec, columns: Seq[Column]): Index = {
    new SimpleIndex(catalog)
  }

  // No DataFrame support
  override def fallback(catalog: Catalog, spec: IndexSpec, condition: Column): DataFrame = {
    null
  }
}
