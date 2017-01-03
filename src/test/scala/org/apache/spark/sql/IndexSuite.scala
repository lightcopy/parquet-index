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

import org.apache.hadoop.fs.Path

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

  test("create index for a Parquet file") {
    withTempDir { dir =>
      withSQLConf(METASTORE_LOCATION.key -> dir.toString / "metastore") {
        spark.range(0, 10).withColumn("str", lit("abc")).coalesce(1).
          write.parquet(dir.toString / "test")
        // find single file that is not a statistics or _SUCCESS
        val status = fs.listStatus(new Path(dir.toString / "test")).
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
        rm(dir.toString / "test", true)

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
        rm(dir.toString / "test", true)

        // should result in no-op since original table already exists
        spark.range(11, 16).withColumn("str", lit("abc")).write.parquet(dir.toString / "test")
        spark.index.create.mode("ignore").indexBy("id", "str").
          table(dir.toString / "test")
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
            table(dir.toString / "test")
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

  test("read correctness for Parquet table (bloom filters) with single equality filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_BLOOM_FILTER_ENABLED.key -> "true") {
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

  test("read correctness for Parquet table (bloom filters) with In filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_BLOOM_FILTER_ENABLED.key -> "true") {
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

  test("read correctness for Parquet table (bloom filters) with 'And' filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_BLOOM_FILTER_ENABLED.key -> "true") {
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

  test("read correctness for Parquet table (bloom filters) with 'Or' filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_BLOOM_FILTER_ENABLED.key -> "true") {
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

  test("read correctness for Parquet table (bloom filters) with null filter") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_BLOOM_FILTER_ENABLED.key -> "true") {
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

  test("read correctness for Parquet table without index filters") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_BLOOM_FILTER_ENABLED.key -> "true") {
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

  test("read correctness for Parquet table with non-equality filters") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_BLOOM_FILTER_ENABLED.key -> "true") {
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

  test("fail to read when array column is used for indexing Parquet table") {
    withTempDir { dir =>
      withSQLConf(
          METASTORE_LOCATION.key -> dir.toString / "metastore",
          PARQUET_BLOOM_FILTER_ENABLED.key -> "true") {
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
          PARQUET_BLOOM_FILTER_ENABLED.key -> "true") {
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
        val df = Seq("a", "é").toDF("name").coalesce(1).write.parquet(dir.toString /"utf")

        spark.index.create.indexBy("name").parquet(dir.toString / "utf")
        val df1 = spark.index.parquet(dir.toString / "utf").filter("name > 'a'")
        df1.collect should be (Array(Row("é")))
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
        val df = Seq("aa", "bé", "bb").toDF("name").coalesce(1).write.parquet(dir.toString /"utf")

        spark.index.create.indexBy("name").parquet(dir.toString / "utf")
        val df1 = spark.index.parquet(dir.toString / "utf").filter("name > 'bb'")
        df1.collect should be (Array(Row("bé")))
        // scalastyle:on
      }
    }
  }
}
