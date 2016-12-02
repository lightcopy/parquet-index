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

package com.github.lightcopy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.sql.{Column, DataFrame}

import com.github.lightcopy.index.Index

/** Simple catalog for local file system with dummy method implementations for testing */
class SimpleCatalog extends Catalog {
  override def fs: FileSystem = FileSystem.get(new Configuration(false))
  override def metastoreLocation: String = "file:/tmp/metastore"
  override def getFreshIndexLocation(): String = "file:/tmp/metastore/test-index"
  override def listIndexes(): Seq[Index] = Seq.empty
  override def getIndex(indexSpec: IndexSpec): Option[Index] = None
  override def createIndex(indexSpec: IndexSpec, columns: Seq[Column]): Unit = { }
  override def dropIndex(indexSpec: IndexSpec): Unit = { }
  override def queryIndex(indexSpec: IndexSpec, condition: Column): DataFrame = null
  override def refreshIndex(indexSpec: IndexSpec): Unit = { }
}
