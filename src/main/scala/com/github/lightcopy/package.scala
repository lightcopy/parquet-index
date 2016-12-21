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

import org.apache.spark.sql.{DataFrameIndexManager, SparkSession}
import org.apache.spark.sql.execution.datasources.IndexSourceStrategy

/** Implicit methods for index */
package object implicits {
  /** [[QueryContext]] to access index functionality using SparkSession */
  implicit class QueryContext(session: SparkSession) {
    def index: DataFrameIndexManager = {
      // check that index strategy is included
      val strategies = session.experimental.extraStrategies
      if (!strategies.contains(IndexSourceStrategy)) {
        session.experimental.extraStrategies = strategies :+ IndexSourceStrategy
      }

      new DataFrameIndexManager(session)
    }
  }
}
