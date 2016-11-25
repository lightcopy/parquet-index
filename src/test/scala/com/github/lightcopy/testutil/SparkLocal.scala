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

package com.github.lightcopy.testutil

import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Spark context with master "local[4]" */
trait SparkLocal extends SparkBase {
  /** Loading Spark configuration for local mode */
  private def localConf: SparkConf = {
    new SparkConf().
      setMaster("local[4]").
      setAppName("spark-local-test").
      set("spark.driver.memory", "1g").
      set("spark.executor.memory", "2g")
  }

  override def createSparkSession(): SparkSession = {
    SparkSession.builder().config(localConf).getOrCreate()
  }
}
