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

package org.apache.spark.sql

import java.io.FileNotFoundException

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.internal.IndexConf._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

import com.github.lightcopy.implicits._
import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class IndexSuite extends UnitTestSuite with SparkLocal {
  // Reset SparkSession for every test, because Metastore caches instance per session, and we
  // do not reset options per metastore configuration.
  before {
    startSparkSession()
  }

  after {
    stopSparkSession()
  }

  //////////////////////////////////////////////////////////////
  // Create index
  //////////////////////////////////////////////////////////////

  test("create index for a Parquet file") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 10).withColumn("str", lit("abc")).coalesce(1).
          write.parquet(dir.toString / "test")
        // find single file that is not a statistics or _SUCCESS
        val status = fs.listStatus(dir / "test").
          filter(_.getPath.getName.contains("part-")).head
        spark.index.create.indexBy("id", "str").parquet(status.getPath.toString)
        spark.index.exists.parquet(status.getPath.toString) should be (true)
      }
    }
  }

  test("create index for a Parquet table") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 10).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        spark.index.exists.parquet(dir.toString / "test") should be (true)
      }
    }
  }

  test("create index for a partitioned Parquet table") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 10).withColumn("str", lit("abc")).
          write.partitionBy("id").parquet(dir.toString / "test")
        spark.index.create.indexBy("str").parquet(dir.toString / "test")
        spark.index.exists.parquet(dir.toString / "test") should be (true)
      }
    }
  }

  test("fail to create if index schema has partitioning column") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 10).withColumn("str", lit("abc")).
          write.partitionBy("id").parquet(dir.toString / "test")
        val err = intercept[IllegalArgumentException] {
          spark.index.create.indexBy("str", "id").parquet(dir.toString / "test")
        }
        assert(err.getMessage.contains("Found column id in partitioning schema. " +
          "Currently indexing of partitioning columns is not supported"))
      }
    }
  }

  test("fail to create if index schema contains non-existing column") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 10).write.parquet(dir.toString / "test")
        val err = intercept[IllegalArgumentException] {
          spark.index.create.indexBy("str", "id").parquet(dir.toString / "test")
        }
        assert(err.getMessage.contains("Failed to select indexed columns. Column str does not " +
          "exist in inferred schema struct<id:bigint>"))
      }
    }
  }

  test("create Parquet index with all available columns in the table") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(5).withColumn("str", lit("abc")).withColumn("num", lit(1)).
          write.parquet(dir.toString / "table")
        spark.index.create.indexByAll.parquet(dir.toString / "table")
        spark.index.exists.parquet(dir.toString / "table") should be (true)
        val df = spark.index.parquet(dir.toString / "table")
        df.schema should be (StructType(
          StructField("id", LongType) ::
          StructField("str", StringType) ::
          StructField("num", IntegerType) :: Nil))
      }
    }
  }

  test("create Parquet index with overwrite mode") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        spark.index.exists.parquet(dir.toString / "test") should be (true)

        // delete table
        rm(dir / "test", true)

        // create index for the different table with overwrite mode
        spark.range(11, 16).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.create.mode("overwrite").indexBy("id", "str").parquet(dir.toString / "test")
        spark.index.exists.parquet(dir.toString / "test") should be (true)
        // check result by quering on different id
        spark.index.parquet(dir.toString / "test").filter(col("id") === 12).count should be (1)
      }
    }
  }

  test("create Parquet index with ignore mode") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        spark.index.exists.parquet(dir.toString / "test") should be (true)

        // delete table
        rm(dir / "test", true)

        // should result in no-op since original table already exists
        spark.range(11, 16).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.create.mode("ignore").indexBy("id", "str").parquet(dir.toString / "test")
        spark.index.exists.parquet(dir.toString / "test") should be (true)
        // check result by quering on different id
        spark.index.parquet(dir.toString / "test").filter(col("id") === 12).count should be (0)
      }
    }
  }

  test("create Parquet index with append mode") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        // append mode is not supported by Parquet right now
        val err = intercept[UnsupportedOperationException] {
          spark.index.create.mode("append").indexBy("id", "str").
            parquet(dir.toString / "test")
        }
        assert(err.getMessage.contains(
          "ParquetMetastoreSupport does not support append to existing index"))
      }
    }
  }

  test("create index if one does not already exist in metastore when loading") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          CREATE_IF_NOT_EXISTS.key -> "true") {
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "table")
        spark.index.exists.parquet(dir.toString / "table") should be (false)
        val df = spark.index.parquet(dir.toString / "table").filter(col("id") === 1)
        df.count should be (1)
        spark.index.exists.parquet(dir.toString / "table") should be (true)
      }
    }
  }

  //////////////////////////////////////////////////////////////
  // Delete index
  //////////////////////////////////////////////////////////////

  test("delete existing Parquet index") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        spark.index.exists.parquet(dir.toString / "test") should be (true)
        spark.index.delete.parquet(dir.toString / "test")
        spark.index.exists.parquet(dir.toString / "test") should be (false)
      }
    }
  }

  test("delete non-existing Parquet index") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.exists.parquet(dir.toString / "test") should be (false)
        spark.index.delete.parquet(dir.toString / "test")
        spark.index.exists.parquet(dir.toString / "test") should be (false)
      }
    }
  }

  test("fail to delete non-existing Parquet index for invalid path") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        intercept[FileNotFoundException] {
          spark.index.delete.parquet(dir.toString / "test")
        }
      }
    }
  }

  //////////////////////////////////////////////////////////////
  // Read correctness with Bloom filter statistics
  //////////////////////////////////////////////////////////////

  test("[bloom] read correctness for Parquet table with single equality filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "bloom") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(col("id") === 1)
        val df2 = spark.read.parquet(dir.toString / "test").filter(col("id") === 1)
        checkAnswer(df1, df2)
      }
    }
  }

  test("[bloom] read correctness for Parquet table with In filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "bloom") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(col("id").isin(1, 3))
        val df2 = spark.read.parquet(dir.toString / "test").filter(col("id").isin(1, 3))
        checkAnswer(df1, df2)
      }
    }
  }

  test("[bloom] read correctness for Parquet table with 'And' filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "bloom") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").
          filter(col("id") === 1 && col("str") === "999")
        val df2= spark.read.parquet(dir.toString / "test").
          filter(col("id") === 1 && col("str") === "999")
        checkAnswer(df1, df2)
      }
    }
  }

  test("[bloom] read correctness for Parquet table with 'Or' filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "bloom") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").
          filter(col("id") === 1 || col("str") === "999")
        val df2= spark.read.parquet(dir.toString / "test").
          filter(col("id") === 1 || col("str") === "999")
        checkAnswer(df1, df2)
      }
    }
  }

  test("[bloom] read correctness for Parquet table with null filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "bloom") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(col("str").isNull)
        val df2 = spark.index.parquet(dir.toString / "test").filter(col("str").isNull)
        checkAnswer(df1, df2)
      }
    }
  }

  test("[bloom] read correctness for Parquet table with non-equality filters") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "bloom") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").
          filter(col("id") > 900 || col("id") < 2)
        val df2 = spark.read.parquet(dir.toString / "test").
          filter(col("id") > 900 || col("id") < 2)
        checkAnswer(df1, df2)
      }
    }
  }

  test("[bloom] read correctness for Parquet table with Date/Timestamp Eq filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "bloom") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = Seq(
          (new java.sql.Date(200000000L), new java.sql.Timestamp(110000000000L), "abc", 1),
          (new java.sql.Date(300000000L), new java.sql.Timestamp(220000000000L), "def", 2),
          (new java.sql.Date(400000000L), new java.sql.Timestamp(330000000000L), "ghi", 3)
        ).toDF("col1", "col2", "col3", "col4")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("col1", "col2").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(
          col("col1") === new java.sql.Date(300000000L) ||
          col("col2") === new java.sql.Timestamp(330000000000L))
        val df2 = spark.read.parquet(dir.toString / "test").filter(
          col("col1") === new java.sql.Date(300000000L) ||
          col("col2") === new java.sql.Timestamp(330000000000L))
        checkAnswer(df1, df2)
      }
    }
  }

  test("[bloom] read correctness for Parquet table with Date/Timestamp GreaterThan filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "bloom") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = Seq(
          (new java.sql.Date(200000000L), new java.sql.Timestamp(110000000000L), "abc", 1),
          (new java.sql.Date(300000000L), new java.sql.Timestamp(220000000000L), "def", 2),
          (new java.sql.Date(400000000L), new java.sql.Timestamp(330000000000L), "ghi", 3)
        ).toDF("col1", "col2", "col3", "col4")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("col1", "col2").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(
          col("col1") > new java.sql.Date(300000000L) &&
          col("col2") >= new java.sql.Timestamp(330000000000L))
        val df2 = spark.read.parquet(dir.toString / "test").filter(
          col("col1") > new java.sql.Date(300000000L) &&
          col("col2") >= new java.sql.Timestamp(330000000000L))
        checkAnswer(df1, df2)
      }
    }
  }

  //////////////////////////////////////////////////////////////
  // Read correctness with Dictionary filter statistics
  //////////////////////////////////////////////////////////////

  test("[dictionary] read correctness for Parquet table with single equality filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "dict") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(col("id") === 1)
        val df2 = spark.read.parquet(dir.toString / "test").filter(col("id") === 1)
        checkAnswer(df1, df2)
      }
    }
  }

  test("[dictionary] read correctness for Parquet table with In filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "dict") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(col("id").isin(1, 3))
        val df2 = spark.read.parquet(dir.toString / "test").filter(col("id").isin(1, 3))
        checkAnswer(df1, df2)
      }
    }
  }

  test("[dictionary] read correctness for Parquet table with 'And' filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "dict") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").
          filter(col("id") === 1 && col("str") === "999")
        val df2= spark.read.parquet(dir.toString / "test").
          filter(col("id") === 1 && col("str") === "999")
        checkAnswer(df1, df2)
      }
    }
  }

  test("[dictionary] read correctness for Parquet table with 'Or' filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "dict") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").
          filter(col("id") === 1 || col("str") === "999")
        val df2= spark.read.parquet(dir.toString / "test").
          filter(col("id") === 1 || col("str") === "999")
        checkAnswer(df1, df2)
      }
    }
  }

  test("[dictionary] read correctness for Parquet table with null filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "dict") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(col("str").isNull)
        val df2 = spark.index.parquet(dir.toString / "test").filter(col("str").isNull)
        checkAnswer(df1, df2)
      }
    }
  }

  test("[dictionary] read correctness for Parquet table with non-equality filters") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "dict") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").
          filter(col("id") > 900 || col("id") < 2)
        val df2 = spark.read.parquet(dir.toString / "test").
          filter(col("id") > 900 || col("id") < 2)
        checkAnswer(df1, df2)
      }
    }
  }

  test("[dictionary] read correctness for Parquet table with Date/Timestamp Eq filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "dict") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = Seq(
          (new java.sql.Date(200000000L), new java.sql.Timestamp(110000000000L), "abc", 1),
          (new java.sql.Date(300000000L), new java.sql.Timestamp(220000000000L), "def", 2),
          (new java.sql.Date(400000000L), new java.sql.Timestamp(330000000000L), "ghi", 3)
        ).toDF("col1", "col2", "col3", "col4")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("col1", "col2").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(
          col("col1") === new java.sql.Date(300000000L) ||
          col("col2") === new java.sql.Timestamp(330000000000L))
        val df2 = spark.read.parquet(dir.toString / "test").filter(
          col("col1") === new java.sql.Date(300000000L) ||
          col("col2") === new java.sql.Timestamp(330000000000L))
        checkAnswer(df1, df2)
      }
    }
  }

  test("[dictionary] read correctness for Parquet table with Date/Timestamp GreaterThan filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true",
          PARQUET_FILTER_STATISTICS_TYPE.key -> "dict") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = Seq(
          (new java.sql.Date(200000000L), new java.sql.Timestamp(110000000000L), "abc", 1),
          (new java.sql.Date(300000000L), new java.sql.Timestamp(220000000000L), "def", 2),
          (new java.sql.Date(400000000L), new java.sql.Timestamp(330000000000L), "ghi", 3)
        ).toDF("col1", "col2", "col3", "col4")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("col1", "col2").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(
          col("col1") > new java.sql.Date(300000000L) &&
          col("col2") >= new java.sql.Timestamp(330000000000L))
        val df2 = spark.read.parquet(dir.toString / "test").filter(
          col("col1") > new java.sql.Date(300000000L) &&
          col("col2") >= new java.sql.Timestamp(330000000000L))
        checkAnswer(df1, df2)
      }
    }
  }

  //////////////////////////////////////////////////////////////
  // General read correctness tests
  //////////////////////////////////////////////////////////////

  test("read correctness for Parquet table with single equality filter") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(col("id") === 1)
        val df2 = spark.read.parquet(dir.toString / "test").filter(col("id") === 1)
        checkAnswer(df1, df2)
      }
    }
  }

  test("read correctness for Parquet table with single equality filter using cached catalog") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(col("id") === 1)
        // cache will already contain entry, result should be the same as using fresh loading
        val df2 = spark.index.parquet(dir.toString / "test").filter(col("id") === 1)
        checkAnswer(df1, df2)
      }
    }
  }

  test("read correctness for Parquet table with equality filter that returns 0 rows") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 9).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.create.indexBy("id", "str").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(col("id") === 999)
        val df2 = spark.read.parquet(dir.toString / "test").filter(col("id") === 999)
        checkAnswer(df1, df2)
      }
    }
  }

  test("read correctness for Parquet table with predicate not containing index filters") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "false") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.sparkContext.parallelize(0 until 16, 16).map { id =>
          (id, s"$id") }.toDF("id", "str")
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(col("str") === "999")
        val df2 = spark.read.parquet(dir.toString / "test").filter(col("str") === "999")
        checkAnswer(df1, df2)
      }
    }
  }

  test("fail to read when array column is used for indexing Parquet table") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = Seq(Seq("a", "b", "c")).toDF("arr")
        df.write.parquet(dir.toString / "test")
        val err = intercept[UnsupportedOperationException] {
          spark.index.create.indexBy("arr").parquet(dir.toString / "test")
        }
        assert(err.getMessage.contains("Schema contains unsupported type"))
      }
    }
  }

  test("fail to read when struct column is used for indexing Parquet table") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = Seq(("a", (1, 2))).toDF("c1", "c2")
        df.write.parquet(dir.toString / "test")
        val err = intercept[UnsupportedOperationException] {
          spark.index.create.indexBy("c1", "c2").parquet(dir.toString / "test")
        }
        assert(err.getMessage.contains("Schema contains unsupported type"))
      }
    }
  }

  test("create and query index for table with all-nulls columns") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = spark.range(16).withColumn("nl", lit(null).cast("string"))
        df.write.parquet(dir.toString / "test")

        spark.index.create.indexBy("id", "nl").parquet(dir.toString / "test")
        val df1 = spark.index.parquet(dir.toString / "test").filter(col("nl") === "a")
        df1.count should be (0)
        val df2 = spark.index.parquet(dir.toString / "test").filter(col("nl").isNull)
        df2.count should be (16)
      }
    }
  }

  test("#25 - create index for UTF-8 statistics, where min > max in Parquet stats") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        // scalastyle:off
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        Seq("a", "é").toDF("col").coalesce(1).write.parquet(dir.toString /"utf")

        spark.index.create.indexBy("col").parquet(dir.toString / "utf")
        val df = spark.index.parquet(dir.toString / "utf").filter("col > 'a'")
        df.collect should be (Array(Row("é")))
        // scalastyle:on
      }
    }
  }

  test("#25 - create index for UTF-8 statistics, where min and max are ascii") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        // scalastyle:off
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        Seq("aa", "bé", "bb").toDF("col").coalesce(1).write.parquet(dir.toString / "utf")

        spark.index.create.indexBy("col").parquet(dir.toString / "utf")
        val df = spark.index.parquet(dir.toString / "utf").filter("col > 'bb'")
        df.collect should be (Array(Row("bé")))
        // scalastyle:on
      }
    }
  }

  test("#25 - create index for table with UTF-8 columns only") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true") {
        // scalastyle:off
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        Seq("ᚠᛇᚻ", "᛫ᛒᛦᚦ᛫ᚠᚱ", "ᚩᚠᚢᚱ᛫", "ᚠᛁᚱᚪ᛫ᚷ", "ᛖᚻᚹᛦ", "ᛚᚳᚢᛗ").toDF("col").
          write.parquet(dir.toString / "utf")

        spark.index.create.indexBy("col").parquet(dir.toString / "utf")
        val df = spark.index.parquet(dir.toString / "utf").filter("col = 'ᛖᚻᚹᛦ'")
        df.collect should be (Array(Row("ᛖᚻᚹᛦ")))
        // scalastyle:on
      }
    }
  }

  test("#40 - query indexed table with empty partitions (files on disk)") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        // DataFrame contains some partitions that are empty (for odd ids)
        spark.sparkContext.parallelize(0 until 8, 8).
          map { x => (x, s"$x") }.
          filter { x => x._1 % 2 == 0 }.toDF("col1", "col2").
          write.parquet(dir.toString / "empt")

        spark.index.create.indexByAll.parquet(dir.toString / "empt")
        val df = spark.index.parquet(dir.toString / "empt").filter("col1 = 2")
        df.collect should be (Array(Row(2, "2")))
      }
    }
  }

  test("#40 - index and query empty table") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        // DataFrame with each partition being empty
        spark.sparkContext.parallelize(0 until 8, 8).
          map { x => (x, s"$x") }.
          filter { x => false }.toDF("col1", "col2").
          write.parquet(dir.toString / "empt")

        spark.index.create.indexByAll.parquet(dir.toString / "empt")
        val df = spark.index.parquet(dir.toString / "empt").filter("col1 = 2")
        df.count should be (0)
      }
    }
  }

  test("index and query table with string column of all empty values") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        // DataFrame with each partition being empty
        Seq("", "", "", "").toDF("col").write.parquet(dir.toString / "str-empty")

        spark.index.create.indexByAll.parquet(dir.toString / "str-empty")
        val df = spark.index.parquet(dir.toString / "str-empty").filter("col = ''")
        df.count should be (4)
      }
    }
  }

  test("index and query table with string column of some empty values") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_FILTER_STATISTICS_ENABLED.key -> "true") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        // DataFrame with each partition being empty
        Seq("", "1", "", "1").toDF("col").write.parquet(dir.toString / "str-empty")

        spark.index.create.indexByAll.parquet(dir.toString / "str-empty")
        val df = spark.index.parquet(dir.toString / "str-empty")
        df.filter("col = ''").count should be (2)
        df.filter("col = '1'").count should be (2)
        df.filter("col > ''").count should be (2)
      }
    }
  }

  //////////////////////////////////////////////////////////////
  // General schema correctness tests
  //////////////////////////////////////////////////////////////

  test("index schema and original Parquet schema have metadata") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        // create table with metadata
        val schema = StructType(Nil).
          add("id", LongType, false, new MetadataBuilder().putString("key", "long col").build()).
          add("name", StringType, true, new MetadataBuilder().putString("key", "str col").build())
        val rdd = spark.sparkContext.
          parallelize(Row(1L, "a") :: Row(2L, "b") :: Row(3L, "c") :: Nil)
        val df = spark.createDataFrame(rdd, schema)
        df.write.parquet(dir.toString / "table-with-metadata")
        // build index for that table
        spark.index.create.indexByAll.parquet(dir.toString / "table-with-metadata")

        val df1 = spark.index.parquet(dir.toString / "table-with-metadata")
        val df2 = spark.read.parquet(dir.toString / "table-with-metadata")
        df1.schema should be (df2.schema)
        df1.schema.fields.map(_.metadata) should be (df2.schema.fields.map(_.metadata))
      }
    }
  }

  test("index schema and original Parquet schema do not have metadata") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        // create table with metadata
        val schema = StructType(Nil).
          add("id", LongType, false).
          add("name", StringType, true)
        val rdd = spark.sparkContext.
          parallelize(Row(1L, "a") :: Row(2L, "b") :: Row(3L, "c") :: Nil)
        val df = spark.createDataFrame(rdd, schema)
        df.write.parquet(dir.toString / "table-no-metadata")
        // build index for that table
        spark.index.create.indexByAll.parquet(dir.toString / "table-no-metadata")

        val df1 = spark.index.parquet(dir.toString / "table-no-metadata")
        val df2 = spark.read.parquet(dir.toString / "table-no-metadata")
        df1.schema should be (df2.schema)
        df1.schema.fields.map(_.metadata) should be (df2.schema.fields.map(_.metadata))
      }
    }
  }

  //////////////////////////////////////////////////////////////
  // Catalog table index tests
  //////////////////////////////////////////////////////////////

  test("index catalog table in Parquet format") {
    withTempDir { dir =>
      withSQLConf(
          "spark.sql.sources.default" -> "parquet",
          METASTORE_LOCATION.key -> dir.toString / "metastore") {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df = Seq(
          ("a", 1, true),
          ("b", 2, false),
          ("c", 3, true)
        ).toDF("col1", "col2", "col3")

        val tableName = "test_parquet_table"
        df.write.saveAsTable(tableName)
        try {
          spark.index.create.indexByAll.table(tableName)
          spark.index.exists.table(tableName) should be (true)
          val res1 = spark.index.table(tableName).filter("col1 = 2")
          val res2 = spark.table(tableName).filter("col1 = 2")
          checkAnswer(res1, res2)
          spark.index.delete.table(tableName)
          spark.index.exists.table(tableName) should be (false)
        } finally {
          spark.sql(s"drop table $tableName")
        }
      }
    }
  }

  test("create different index for catalog table and datasource with same path") {
    withTempDir { dir =>
      withSQLConf(
          "spark.sql.sources.default" -> "parquet",
          METASTORE_LOCATION.key -> dir.toString / "metastore") {
        val df = spark.range(0, 10)
        val tableName = "test_parquet_table"
        df.write.saveAsTable(tableName)
        val tableLocation = spark.conf.get("spark.sql.warehouse.dir").
          replace("${system:user.dir}", fs.getWorkingDirectory.toString) / "test_parquet_table"
        try {
          spark.index.create.indexByAll.table(tableName)
          spark.index.create.indexByAll.parquet(tableLocation)

          spark.index.exists.table(tableName) should be (true)
          spark.index.exists.parquet(tableLocation) should be (true)
        } finally {
          spark.sql(s"drop table $tableName")
        }
      }
    }
  }

  test("overwrite index for catalog table") {
    withTempDir { dir =>
      withSQLConf(
          "spark.sql.sources.default" -> "parquet",
          METASTORE_LOCATION.key -> dir.toString / "metastore") {
        val df = spark.range(0, 10).withColumn("col1", lit("abc"))
        val tableName = "test_parquet_table"
        df.write.saveAsTable(tableName)
        try {
          spark.index.create.indexByAll.table(tableName)
          spark.index.exists.table(tableName) should be (true)
          spark.index.create.mode("overwrite").indexBy("id").table(tableName)
          spark.index.exists.table(tableName) should be (true)
        } finally {
          spark.sql(s"drop table $tableName")
        }
      }
    }
  }

  test("fail to query index when underlying catalog table is dropped") {
    withTempDir { dir =>
      withSQLConf(
          "spark.sql.sources.default" -> "parquet",
          METASTORE_LOCATION.key -> dir.toString / "metastore") {
        val df = spark.range(0, 10).withColumn("col1", lit("abc"))
        val tableName = "test_parquet_table"
        df.write.saveAsTable(tableName)
        try {
          spark.index.create.indexByAll.table(tableName)
        } finally {
          spark.sql(s"drop table $tableName")
        }

        val err = intercept[NoSuchTableException] {
          spark.index.table(tableName)
        }
        err.getMessage.contains(s"Table or view '$tableName' not found in database")
      }
    }
  }
}
