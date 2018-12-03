/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// todo remove this, just for demo.
object SparkPartitionIndexExample {

  def main(args: Array[String]) {

    val path = System.getProperty("java.io.tmpdir")
    new File(path).deleteOnExit()

    val spark = SparkSession
      .builder()
      .appName("Spark Paruqet example")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    spark.range(0, 100 * 10000).
      select(col("id"), col("id").cast("string").as("code"), lit("xyz").as("name")).
      repartition(10, col("id")).
      write.partitionBy("name").parquet(path)

    import com.github.lightcopy.implicits._
    require(spark.index.repartition("id").parquet(path)
      .filter(col("id") === 123 && col("code") === "123").collect().length == 1)
    spark.close()
  }

}
