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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.parquet.column.statistics._
import org.apache.parquet.schema.MessageTypeParser

import org.apache.spark.SparkException
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter

import com.github.lightcopy.util.SerializableFileStatus
import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class ParquetStatisticsRDDSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("ParquetStatisticsPartition - hashCode") {
    // sequence of statuses is not part of the hashcode
    val part1 = new ParquetStatisticsPartition(123L, 0, Seq.empty)
    val part2 = new ParquetStatisticsPartition(123L, 1, Seq(null))
    val part3 = new ParquetStatisticsPartition(123L, 0, Seq(null))
    assert(part1.hashCode != part2.hashCode)
    assert(part1.hashCode == part3.hashCode)
  }

  test("ParquetStatisticsPartition - equals") {
    val part1 = new ParquetStatisticsPartition(123L, 0, Seq.empty)
    val part2 = new ParquetStatisticsPartition(123L, 1, Seq(null))
    val part3 = new ParquetStatisticsPartition(123L, 0, Seq(null))

    part1.equals(part2) should be (false)
    part1.equals(part3) should be (true)
    part1.equals(null) should be (false)
    part1.equals("str") should be (false)
  }

  test("ParquetStatisticsPartition - iterator") {
    val part1 = new ParquetStatisticsPartition(123L, 0, Seq.empty)
    val part2 = new ParquetStatisticsPartition(123L, 1, Seq(null))
    part1.iterator.isEmpty should be (true)
    part2.iterator.isEmpty should be (false)
    part2.iterator.next should be (null)
  }

  test("ParquetStatisticsRDD - partitionData, fail for non-positive slices") {
    var err = intercept[IllegalArgumentException] {
      ParquetStatisticsRDD.partitionData(Seq(1, 2, 3), -1)
    }
    assert(err.getMessage.contains("Positive number of slices required, found -1"))

    err = intercept[IllegalArgumentException] {
      ParquetStatisticsRDD.partitionData(Seq(1, 2, 3), 0)
    }
    assert(err.getMessage.contains("Positive number of slices required, found 0"))
  }

  test("ParquetStatisticsRDD - partitionData, partition per element") {
    val seq = ParquetStatisticsRDD.partitionData(Seq(1, 2, 3), 3)
    seq should be (Seq(Seq(1), Seq(2), Seq(3)))
  }

  test("ParquetStatisticsRDD - convertStatistics, integer stats") {
    val stats = new IntStatistics()
    stats.setMinMax(1, 2)
    stats.setNumNulls(5)
    ParquetStatisticsRDD.convertStatistics(stats) should be (ParquetIntStatistics(1, 2, 5))
  }

  test("ParquetStatisticsRDD - convertStatistics, long stats") {
    val stats = new LongStatistics()
    stats.setMinMax(1L, 2L)
    stats.setNumNulls(5)
    ParquetStatisticsRDD.convertStatistics(stats) should be (ParquetLongStatistics(1L, 2L, 5))
  }

  test("ParquetStatisticsRDD - convertStatistics, string stats") {
    val stats = new BinaryStatistics()
    stats.setMinMaxFromBytes("a".getBytes(), "b".getBytes())
    stats.setNumNulls(5)
    ParquetStatisticsRDD.convertStatistics(stats) should be (ParquetStringStatistics("a", "b", 5))
  }

  test("ParquetStatisticsRDD - convertStatistics, unsupported stats") {
    val stats = new BooleanStatistics()
    // non-initialized statistics are converted into null statistics
    stats.initializeStats(false, true)
    val err = intercept[UnsupportedOperationException] {
      ParquetStatisticsRDD.convertStatistics(stats)
    }
    assert(err.getMessage.
      contains("Statistics min: false, max: true, num_nulls: 0 is not supported"))
  }

  test("ParquetStatisticsRDD - convertStatistics, undefined min/max") {
    val stats = new IntStatistics()
    stats.setNumNulls(5)
    ParquetStatisticsRDD.convertStatistics(stats) should be (ParquetNullStatistics(5))
  }

  test("ParquetStatisticsRDD - convertStatistics, undefined min/max, with 0 nulls") {
    val stats = new BinaryStatistics()
    ParquetStatisticsRDD.convertStatistics(stats) should be (ParquetNullStatistics(0))
  }

  test("ParquetStatisticsRDD - prepareBloomFilter, empty blocks array") {
    val schema = MessageTypeParser.parseMessageType(
      """
      | message spark_schema {
      |   required int64 id;
      |   required binary str (UTF8);
      | }
      """.stripMargin)
    ParquetStatisticsRDD.prepareBloomFilter(schema, Array.empty) should be (Map.empty)
  }

  test("ParquetStatisticsRDD - prepareBloomFilter, non-empty blocks") {
    val schema = MessageTypeParser.parseMessageType(
      """
      | message spark_schema {
      |   required int64 id;
      |   required binary str (UTF8);
      | }
      """.stripMargin)
    val blocks = Array(
      ParquetBlockMetadata(100, Map(
        "id" -> ParquetColumnMetadata("id", 100, null, None),
        "str" -> ParquetColumnMetadata("str", 100, null, None))
      )
    )
    val res = ParquetStatisticsRDD.prepareBloomFilter(schema, blocks)
    res.size should be (2)
    res(0)._1 should be ("id")
    res(0)._2.isInstanceOf[BloomFilter] should be (true)
    res(1)._1 should be ("str")
    res(1)._2.isInstanceOf[BloomFilter] should be (true)
  }

  test("ParquetStatisticsRDD - prepareBloomFilter, schema is larger than indexed columns") {
    val schema = MessageTypeParser.parseMessageType(
      """
      | message spark_schema {
      |   required int64 id;
      |   required int64 index;
      |   required binary str (UTF8);
      | }
      """.stripMargin)
    val blocks = Array(
      ParquetBlockMetadata(100, Map(
        "id" -> ParquetColumnMetadata("id", 100, null, None),
        "str" -> ParquetColumnMetadata("str", 100, null, None))
      )
    )

    val res = ParquetStatisticsRDD.prepareBloomFilter(schema, blocks)
    res.size should be (2)
    res(0)._1 should be ("id")
    res(2)._1 should be ("str")
  }

  test("ParquetStatisticsRDD - withBloomFilters, create bloom filter") {
    withTempDir { dir =>
      spark.range(0, 10).withColumn("str", lit("abc")).coalesce(1).
        write.parquet(dir.toString / "table")

      val status = fs.listStatus(new Path(dir.toString / "table")).
        filter(_.getPath.getName.contains("parquet")).head

      val schema = MessageTypeParser.parseMessageType(
        """
        | message spark_schema {
        |   required int64 id;
        |   required binary str (UTF8);
        | }
        """.stripMargin)

      val blocks = Array(
        ParquetBlockMetadata(10, Map(
          "id" -> ParquetColumnMetadata("id", 10, null, None),
          "str" -> ParquetColumnMetadata("str", 10, null, None))
        )
      )

      val filterDir = fs.getFileStatus(dir)
      val context = ParquetStatisticsRDD.taskAttemptContext(new Configuration(false), 1)
      val res = ParquetStatisticsRDD.withBloomFilters(schema, context, blocks, status, filterDir)

      res.length should be (1)
      val cols = res.head.indexedColumns
      cols.size should be (2)

      cols("id").fieldName should be ("id")
      cols("id").filter.isDefined should be (true)
      val filter1 = cols("id").filter.get
      filter1.init(new Configuration(false))
      for (i <- 0 until 10) {
        filter1.mightContain(i.toLong) should be (true)
      }

      cols("str").fieldName should be ("str")
      cols("str").filter.isDefined should be (true)
      val filter2 = cols("str").filter.get
      filter2.init(new Configuration(false))
      filter2.mightContain("abc") should be (true)
    }
  }

  test("ParquetStatisticsRDD - withBloomFilters, fail when cannot create directory") {
    withTempDir(new FsPermission("444")) { dir =>
      val context = ParquetStatisticsRDD.taskAttemptContext(new Configuration(false), 1)
      val schema = MessageTypeParser.parseMessageType(
        "message spark_schema { required boolean bool; }")
      val err = intercept[IOException] {
        ParquetStatisticsRDD.withBloomFilters(schema, context, Array.empty, fs.getFileStatus(dir),
          fs.getFileStatus(dir))
      }
      assert(err.getMessage.contains("Failed to create target directory"))
    }
  }

  test("ParquetStatisticsRDD - validate empty index schema") {
    val err = intercept[UnsupportedOperationException] {
      new ParquetStatisticsRDD(
        spark.sparkContext,
        spark.sessionState.newHadoopConf(),
        schema = StructType(Seq.empty),
        data = Seq.empty,
        numPartitions = 8)
    }
    assert(err.getMessage.contains("Empty schema StructType() is not supported"))
  }

  test("ParquetStatisticsRDD - validate unsupported index schema") {
    val err = intercept[UnsupportedOperationException] {
      new ParquetStatisticsRDD(
        spark.sparkContext,
        spark.sessionState.newHadoopConf(),
        schema = StructType(
          StructField("str", StringType) ::
          StructField("a", StructType(Seq.empty)) :: Nil),
        data = Seq.empty,
        numPartitions = 8)
    }
    assert(err.getMessage.contains("Schema contains unsupported type"))
  }

  test("ParquetStatisticsRDD - validate index schema by definition/repetition levels") {
    withTempDir { dir =>
      // all values are in single file
      spark.range(0, 10).write.parquet(dir.toString / "table")
      val status = fs.listStatus(new Path(dir.toString / "table")).
        filter(_.getPath.getName.contains("parquet"))

      val rdd = new ParquetStatisticsRDD(
        spark.sparkContext,
        spark.sessionState.newHadoopConf(),
        // schema will result in optional fields, but table contains required
        schema = StructType(StructField("id", LongType, true) :: Nil),
        data = status.map(SerializableFileStatus.fromFileStatus).toSeq,
        numPartitions = 8)

      val err = intercept[SparkException] {
        rdd.collect
      }
      assert(err.getCause.getMessage.
        contains("optional int64 id found: expected required int64 id"))
    }
  }

  test("ParquetStatisticsRDD - fail to collect stats if file is not a Parquet") {
    withTempDir { dir =>
      val status = fs.getFileStatus(dir)
      val rdd = new ParquetStatisticsRDD(
        spark.sparkContext,
        spark.sessionState.newHadoopConf(),
        schema = StructType(
          StructField("str", StringType) ::
          StructField("id", IntegerType) :: Nil),
        data = Seq(SerializableFileStatus.fromFileStatus(status)),
        numPartitions = 8)

      val err = intercept[SparkException] {
        rdd.collect
      }
      err.getCause.getMessage.contains("Could not read footer")
    }
  }

  test("ParquetStatisticsRDD - field order is irrelevant when collecting stats (bloom filters)") {
    withTempDir { dir =>
      // all values are in single file
      spark.range(0, 10).withColumn("str", lit("abc")).coalesce(1).
        write.parquet(dir.toString / "table")
      val status = fs.listStatus(new Path(dir.toString / "table")).
        filter(_.getPath.getName.contains("parquet"))
      val hadoopConf = spark.sessionState.newHadoopConf()
      hadoopConf.set(ParquetMetastoreSupport.BLOOM_FILTER_DIR, dir.toString)

      val rdd = new ParquetStatisticsRDD(
        spark.sparkContext,
        hadoopConf,
        // schema is reversed compare to actual table schema (id, str)
        schema = StructType(
          StructField("str", StringType, false) ::
          StructField("id", LongType, false) :: Nil),
        data = status.map(SerializableFileStatus.fromFileStatus).toSeq,
        numPartitions = 8)

      val res = rdd.collect
      val cols = res.head.blocks.head.indexedColumns

      val filter1 = cols("id").filter.get
      filter1.init(new Configuration(false))
      filter1.mightContain("abc") should be (false)
      for (i <- 0L until 10L) {
        filter1.mightContain(i) should be (true)
      }

      val filter2 = cols("str").filter.get
      filter2.init(new Configuration(false))
      filter2.mightContain(1L) should be (false)
      filter2.mightContain(9L) should be (false)
      filter2.mightContain("abc") should be (true)
    }
  }
}
