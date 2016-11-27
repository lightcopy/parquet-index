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

package com.github.lightcopy.index

/**
 * Internal column info.
 * @param identifier column identifier/name
 * @param datatype column data type, e.g. "string", "long"
 */
case class ColumnSpec(identifier: String, datatype: String)

/**
 * Index metadata, maps directly to JSON.
 * @param source source to use when loading index, e.g. "parquet"
 * @param path optional path to the datasource table that is used for index
 * @param columns set of column specs that are supported by index
 * @param indexOptions specific index options, depends on implementation
 */
case class Metadata(
  source: String,
  path: Option[String],
  columns: Seq[ColumnSpec],
  indexOptions: Map[String, String])
