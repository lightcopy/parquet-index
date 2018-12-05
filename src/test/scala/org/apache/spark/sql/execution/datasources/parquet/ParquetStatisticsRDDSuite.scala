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

import org.apache.hadoop.fs.FileSystem
import org.apache.parquet.hadoop.metadata.BlockMetaData

import org.apache.spark.SparkException
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import com.github.lightcopy.util.{SerializableBlockLocation, SerializableFileStatus}
import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class ParquetStatisticsRDDSuite extends UnitTestSuite with SparkLocal {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  // Block location with size in bytes and hosts
  def block(size: Long, hosts: String*): SerializableBlockLocation = {
    SerializableBlockLocation(hosts.toArray, hosts.toArray, 1L, size)
  }

  // File status with provided blocks
  def file(blocks: SerializableBlockLocation*): SerializableFileStatus = {
    SerializableFileStatus("path", 1L, false, 1, 128L, 0L, 0L, blockLocations = blocks.toArray)
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

  test("ParquetStatisticsRDD - validate empty index schema") {
    val err = intercept[UnsupportedOperationException] {
      new ParquetStatisticsRDD(
        spark.sparkContext,
        spark.sessionState.newHadoopConf(),
        schema = StructType(Seq.empty),
        spark.sessionState.conf.writeLegacyParquetFormat,
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
        spark.sessionState.conf.writeLegacyParquetFormat,
        data = Seq.empty,
        numPartitions = 8)
    }
    assert(err.getMessage.contains("Schema contains unsupported type"))
  }

  test("ParquetStatisticsRDD - validate index schema by definition/repetition levels") {
    withTempDir { dir =>
      // all values are in single file
      spark.range(0, 10).write.parquet(dir.toString / "table")
      val status = fs.listStatus(dir / "table").filter(_.getPath.getName.contains("parquet"))

      val rdd = new ParquetStatisticsRDD(
        spark.sparkContext,
        spark.sessionState.newHadoopConf(),
        // schema will result in optional fields, but table contains required
        schema = StructType(StructField("id", LongType, true) :: Nil),
        spark.sessionState.conf.writeLegacyParquetFormat,
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
        spark.sessionState.conf.writeLegacyParquetFormat,
        data = Seq(SerializableFileStatus.fromFileStatus(status)),
        numPartitions = 8)

      val err = intercept[SparkException] {
        rdd.collect
      }
      err.getCause.getMessage.contains("Could not read footer")
    }
  }

  test("ParquetStatisticsRDD - schemaBasedStatistics, empty schema") {
    val res = ParquetStatisticsRDD.schemaBasedStatistics(StructType(Nil),
      new BlockMetaData(), new FilterStatisticsMetadata())
    res should be (Map.empty)
  }

  test("ParquetStatisticsRDD - schemaBasedStatistics, duplicated top-level column names") {
    val res = ParquetStatisticsRDD.schemaBasedStatistics(
      StructType(StructField("col1", IntegerType) :: StructField("col1", LongType) :: Nil),
      new BlockMetaData(),
      new FilterStatisticsMetadata())
    res should be (Map("col1" -> (LongColumnStatistics(), None)))
  }

  test("ParquetStatisticsRDD - schemaBasedStatistics, disabled column filters") {
    val res = ParquetStatisticsRDD.schemaBasedStatistics(
      StructType(StructField("col1", IntegerType) :: StructField("col2", LongType) :: Nil),
      new BlockMetaData(),
      new FilterStatisticsMetadata())
    res.size should be (2)
    res.foreach { case (column, (stats, filter)) => filter should be (None) }
  }

  test("ParquetStatisticsRDD - schemaBasedStatistics, enabled column filters") {
    withTempDir { dir =>
      val filterMetadata = new FilterStatisticsMetadata()
      filterMetadata.setDirectory(Some(fs.getFileStatus(dir)))
      filterMetadata.setFilterType(Some("bloom"))

      val block = new BlockMetaData()
      block.setRowCount(123L)

      val res = ParquetStatisticsRDD.schemaBasedStatistics(
        StructType(StructField("col1", IntegerType) :: StructField("col2", LongType) :: Nil),
        block,
        filterMetadata)
      res.size should be (2)
      res.foreach { case (column, (stats, filter)) => filter.isDefined should be (true) }
    }
  }

  test("ParquetStatisticsRDD - convertBlocks, empty sequence") {
    ParquetStatisticsRDD.convertBlocks(Seq.empty) should be (Array.empty)
  }

  test("ParquetStatisticsRDD - convertBlocks, filter is None") {
    val blocks = Seq(
      (123L, Map[Int, (String, ColumnStatistics, Option[ColumnFilterStatistics])](
        0 -> ("a", IntColumnStatistics(), None),
        1 -> ("b", LongColumnStatistics(), None)
      ))
    )
    val res = ParquetStatisticsRDD.convertBlocks(blocks)
    res should be (Array(
      ParquetBlockMetadata(123L, Map(
        "a" -> ParquetColumnMetadata("a", 123L, IntColumnStatistics(), None),
        "b" -> ParquetColumnMetadata("b", 123L, LongColumnStatistics(), None)
      ))
    ))
  }

  test("ParquetStatisticsRDD - convertBlocks, valueCount is the same as rowCount") {
    val stats = IntColumnStatistics()
    stats.updateMinMax(11)
    stats.incrementNumNulls()
    val blocks = Seq(
      (123L, Map[Int, (String, ColumnStatistics, Option[ColumnFilterStatistics])](
        0 -> ("a", stats, None),
        1 -> ("b", stats, None)
      ))
    )
    val res = ParquetStatisticsRDD.convertBlocks(blocks)
    res should be (Array(
      ParquetBlockMetadata(123L, Map(
        "a" -> ParquetColumnMetadata("a", 123L, stats, None),
        "b" -> ParquetColumnMetadata("b", 123L, stats, None)
      ))
    ))
  }

  // This test does not happen in real world schenario, because we ensure that columns are unique
  // But we do not handle this case explicitly, hence test to verify the edge case
  test("ParquetStatisticsRDD - convertBlocks, different index, same column name") {
    // can be different column type
    val blocks = Seq(
      (123L, Map[Int, (String, ColumnStatistics, Option[ColumnFilterStatistics])](
        0 -> ("a", IntColumnStatistics(), None),
        1 -> ("a", IntColumnStatistics(), None)
      ))
    )
    val res = ParquetStatisticsRDD.convertBlocks(blocks)
    res should be (Array(
      ParquetBlockMetadata(123L, Map(
        "a" -> ParquetColumnMetadata("a", 123L, IntColumnStatistics(), None)
      ))
    ))
  }

  test("ParquetStatisticsRDD - newFilterFile, correct column name 1") {
    val name = ParquetStatisticsRDD.newFilterFile(1, "abc")
    assert(name.contains("block00001"))
    assert(name.contains("col_abc"))
  }

  test("ParquetStatisticsRDD - newFilterFile, correct column name 2") {
    val name = ParquetStatisticsRDD.newFilterFile(1, "a.b.c")
    assert(name.contains("block00001"))
    assert(name.contains("col_a.b.c"))
  }

  test("ParquetStatisticsRDD - newFilterFile, block exceeds digits format") {
    val name = ParquetStatisticsRDD.newFilterFile(12345678, "a.b.c")
    assert(name.contains("block12345678"))
    assert(name.contains("col_a.b.c"))
  }

  test("ParquetStatisticsRDD - newFilterFile, column name contains invalid characters") {
    val name = ParquetStatisticsRDD.newFilterFile(123, "a/b/c\\d   e")
    assert(name.contains("block00123"))
    assert(name.contains("col_a_b_c_d___e"))
  }

  test("ParquetStatisticsRDD - collect statistics for empty file") {
    withTempDir { dir =>
      // all values are in single file
      spark.range(0, 1).filter("id > 2").withColumn("str", lit("abc")).coalesce(1).
        write.parquet(dir.toString / "table")
      val status = fs.listStatus(dir / "table").filter(_.getPath.getName.contains("parquet"))

      val rdd = new ParquetStatisticsRDD(
        spark.sparkContext,
        spark.sessionState.newHadoopConf(),
        schema = StructType(
          StructField("id", LongType, false) ::
          StructField("str", StringType, false) :: Nil),
        spark.sessionState.conf.writeLegacyParquetFormat,
        data = status.map(SerializableFileStatus.fromFileStatus).toSeq,
        numPartitions = 4)

      val res = rdd.collect
      res.head.blocks.isEmpty should be (true)
    }
  }

  test("ParquetStatisticsRDD - collect partial-null and full-null fields") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    withTempDir { dir =>
      Seq[(Int, String, String)](
        (0, "a", null),
        (1, "b", null),
        (2, null, null),
        (3, null, null),
        (4, "c", null)
      ).toDF("col1", "col2", "col3").coalesce(1).write.parquet(dir.toString / "table")
      val status = fs.listStatus(dir / "table").filter(_.getPath.getName.contains("parquet"))

      val rdd = new ParquetStatisticsRDD(
        spark.sparkContext,
        spark.sessionState.newHadoopConf(),
        schema = StructType(
          StructField("col1", IntegerType, false) ::
          StructField("col2", StringType, true) ::
          StructField("col3", StringType, true) :: Nil),
        spark.sessionState.conf.writeLegacyParquetFormat,
        data = status.map(SerializableFileStatus.fromFileStatus).toSeq,
        numPartitions = 4)

      val res = rdd.collect
      val cols = res.head.blocks.head.indexedColumns

      val stats1 = cols("col1").stats
      assert(stats1.getMin === 0)
      assert(stats1.getMax === 4)
      assert(stats1.getNumNulls === 0)

      val stats2 = cols("col2").stats
      assert(stats2.getMin === "a")
      assert(stats2.getMax === "c")
      assert(stats2.getNumNulls === 2)

      val stats3 = cols("col3").stats
      assert(stats3.getMin === null)
      assert(stats3.getMax === null)
      assert(stats3.getNumNulls === 5)
    }
  }

  test("ParquetStatisticsRDD - field order is irrelevant when collecting stats/filters") {
    withTempDir { dir =>
      // all values are in single file
      spark.range(0, 10).withColumn("str", lit("abc")).coalesce(1).
        write.parquet(dir.toString / "table")
      val status = fs.listStatus(dir / "table").filter(_.getPath.getName.contains("parquet"))
      val hadoopConf = spark.sessionState.newHadoopConf()
      // enable filter statistics
      hadoopConf.set(ParquetMetastoreSupport.FILTER_DIR, dir.toString)
      hadoopConf.set(ParquetMetastoreSupport.FILTER_TYPE, "bloom")

      val rdd = new ParquetStatisticsRDD(
        spark.sparkContext,
        hadoopConf,
        // schema is reversed compare to actual table schema (id, str)
        schema = StructType(
          StructField("str", StringType, false) ::
          StructField("id", LongType, false) :: Nil),
        spark.sessionState.conf.writeLegacyParquetFormat,
        data = status.map(SerializableFileStatus.fromFileStatus).toSeq,
        numPartitions = 4)

      val res = rdd.collect
      val cols = res.head.blocks.head.indexedColumns

      val stats1 = cols("id").stats
      assert(stats1.getMin === 0)
      assert(stats1.getMax === 9)
      assert(stats1.getNumNulls === 0)

      val stats2 = cols("str").stats
      assert(stats2.getMin === "abc")
      assert(stats2.getMax === "abc")
      assert(stats2.getNumNulls === 0)

      val filter1 = cols("id").filter.get
      filter1.readData(fs)
      filter1.mightContain("abc") should be (false)
      for (i <- 0L until 10L) {
        filter1.mightContain(i) should be (true)
      }

      val filter2 = cols("str").filter.get
      filter2.readData(fs)
      filter2.mightContain(1L) should be (false)
      filter2.mightContain(9L) should be (false)
      filter2.mightContain("abc") should be (true)
    }
  }

  test("ParquetStatisticsRDD - getPreferredLocations, many files with blocks") {
    val partition = new ParquetStatisticsPartition(123, 0,
      file(block(100L, "host1", "host2")) ::
      file(block(200L, "host2", "host3")) ::
      file(block(400L, "host3", "host4")) :: Nil)
    val rdd = new ParquetStatisticsRDD(
      spark.sparkContext,
      spark.sessionState.newHadoopConf(),
      schema = StructType(StructField("a", StringType) :: Nil),
      spark.sessionState.conf.writeLegacyParquetFormat,
      data = Seq.empty,
      numPartitions = 8)

    val hosts = rdd.getPreferredLocations(partition)
    hosts should be (Seq("host3", "host4", "host2"))
  }

  test("ParquetStatisticsRDD - getPreferredLocations, many files with blocks + localhost") {
    val partition = new ParquetStatisticsPartition(123, 0,
      file(block(100L, "localhost", "host2")) ::
      file(block(200L, "host2", "localhost")) ::
      file(block(400L, "localhost", "host4")) :: Nil)
    val rdd = new ParquetStatisticsRDD(
      spark.sparkContext,
      spark.sessionState.newHadoopConf(),
      schema = StructType(StructField("a", StringType) :: Nil),
      spark.sessionState.conf.writeLegacyParquetFormat,
      data = Seq.empty,
      numPartitions = 8)

    val hosts = rdd.getPreferredLocations(partition)
    hosts should be (Seq("host4", "host2"))
  }

  test("ParquetStatisticsRDD - getPreferredLocations, all locations are localhost") {
    val partition = new ParquetStatisticsPartition(123, 0,
      file(block(100L, "localhost")) ::
      file(block(200L, "localhost")) ::
      file(block(400L, "localhost")) :: Nil)
    val rdd = new ParquetStatisticsRDD(
      spark.sparkContext,
      spark.sessionState.newHadoopConf(),
      schema = StructType(StructField("a", StringType) :: Nil),
      spark.sessionState.conf.writeLegacyParquetFormat,
      data = Seq.empty,
      numPartitions = 8)

    val hosts = rdd.getPreferredLocations(partition)
    hosts should be (Nil)
  }

  test("ParquetStatisticsRDD - getPreferredLocations, single file with many blocks") {
    val partition = new ParquetStatisticsPartition(123, 0,
      file(
        block(100L, "localhost", "host1", "host2"),
        block(200L, "localhost", "host2", "host3"),
        block(400L, "localhost", "host4")) :: Nil)
    val rdd = new ParquetStatisticsRDD(
      spark.sparkContext,
      spark.sessionState.newHadoopConf(),
      schema = StructType(StructField("a", StringType) :: Nil),
      spark.sessionState.conf.writeLegacyParquetFormat,
      data = Seq.empty,
      numPartitions = 8)

    val hosts = rdd.getPreferredLocations(partition)
    hosts should be (Seq("host4", "host2", "host3"))
  }

  test("ParquetStatisticsRDD - getPreferredLocations, single file with single block") {
    val partition = new ParquetStatisticsPartition(123, 0,
      file(block(100L, "host1")) :: Nil)
    val rdd = new ParquetStatisticsRDD(
      spark.sparkContext,
      spark.sessionState.newHadoopConf(),
      schema = StructType(StructField("a", StringType) :: Nil),
      spark.sessionState.conf.writeLegacyParquetFormat,
      data = Seq.empty,
      numPartitions = 8)

    val hosts = rdd.getPreferredLocations(partition)
    hosts should be (Seq("host1"))
  }

  test("ParquetStatisticsRDD - getPreferredLocations, single file with empty blocks") {
    val partition = new ParquetStatisticsPartition(123, 0, file() :: Nil)
    val rdd = new ParquetStatisticsRDD(
      spark.sparkContext,
      spark.sessionState.newHadoopConf(),
      schema = StructType(StructField("a", StringType) :: Nil),
      spark.sessionState.conf.writeLegacyParquetFormat,
      data = Seq.empty,
      numPartitions = 8)

    val hosts = rdd.getPreferredLocations(partition)
    hosts should be (Nil)
  }

  test("ParquetStatisticsRDD - getFileSystem, check file://") {
    val hadoopConf = spark.sessionState.newHadoopConf()
    // ideally should have tested s3a here but that needs credentials
    // and also AWS SDK classes in classpath
    hadoopConf.set(ParquetMetastoreSupport.FILTER_DIR, "file://issue-86-test/index_metastore")
    val fs = ParquetStatisticsRDD.getFileSystem(hadoopConf)
    assert(fs.getScheme().equals("file"))
  }
}
