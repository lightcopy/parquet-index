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

import org.apache.spark.sql.types._

/**
 * Utility functions to provide list of supported fields, validate and prune columns.
 */
object ParquetSchemaUtils {
  // Supported top-level Spark SQL data types
  val SUPPORTED_TYPES: Set[DataType] = Set(IntegerType, LongType, StringType)

  /**
   * Validate input schema as `StructType` and throw exception if schema does not match expected
   * column types or is empty (see list of supported fields above). Note that this is used in
   * statistics conversion, so when adding new type, one should update statistics.
   */
  def validateStructType(schema: StructType): Unit = {
    if (schema.isEmpty) {
      throw new UnsupportedOperationException(s"Empty schema $schema is not supported, please " +
        s"provide at least one column of a type ${SUPPORTED_TYPES.mkString("[", ", ", "]")}")
    }
    schema.fields.foreach { field =>
      if (!SUPPORTED_TYPES.contains(field.dataType)) {
        throw new UnsupportedOperationException(
          "Schema contains unsupported type, " +
          s"field=$field, " +
          s"schema=${schema.simpleString}, " +
          s"supported types=${SUPPORTED_TYPES.mkString("[", ", ", "]")}")
      }
    }
  }

  /**
   * Prune invalid columns from StructType leaving only supported data types. Only works for
   * top-level columns at this point.
   */
  def pruneStructType(schema: StructType): StructType = {
    val updatedFields = schema.fields.filter { field =>
      SUPPORTED_TYPES.contains(field.dataType)
    }
    StructType(updatedFields)
  }
}
