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

package com.github.lightcopy.api;

import java.io.File;
import java.io.IOException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;

import com.github.lightcopy.QueryContext;

/**
 * Java API suite for tests with default index configuration.
 */
public class JavaApiSuite {
  // configuration for metastore, should be updated once changed
  static String METASTORE_LOCATION = "spark.sql.index.metastore";
  SparkSession spark;

  @Rule
  public TemporaryFolder dir = new TemporaryFolder();

  @Before
  public void startSparkSession() {
    if (spark != null) {
      stopSparkSession();
    }
    spark = SparkSession.
      builder().
      master("local[*]").
      appName("Java unit-tests").
      getOrCreate();
  }

  @After
  public void stopSparkSession() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  public Dataset<Row> sampleParquet(SparkSession spark, String tablePath) {
    // create sample Parquet table
    Dataset<Row> df = spark.sql(
      "select 1 as col1, 'a' as col2 union " +
      "select 2 as col1, 'b' as col2 union " +
      "select 3 as col1, 'c' as col2").coalesce(1);
    df.write().parquet(tablePath);
    return df;
  }

  @Test
  public void testCreateIndexByAll() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    context.index().create().indexByAll().parquet(tablePath.toString());
    boolean exists = context.index().exists().parquet(tablePath.toString());
    assertEquals(exists, true);
  }

  @Test
  public void testCreateIndexByColumnNames() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    context.index().create().indexBy(new String[] { "col1", "col2" }).parquet(tablePath.toString());
    boolean exists = context.index().exists().parquet(tablePath.toString());
    assertEquals(exists, true);
  }

  @Test
  public void testCreateIndexByColumns() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    Column[] columns = new Column[] { new Column("col1"), new Column("col2") };
    context.index().create().indexBy(columns).parquet(tablePath.toString());
    boolean exists = context.index().exists().parquet(tablePath.toString());
    assertEquals(exists, true);
  }

  @Test
  public void testCreateIndexByOverwriteMode() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    context.index().create().mode("overwrite").indexByAll().parquet(tablePath.toString());
    context.index().create().mode(SaveMode.Overwrite).indexByAll().parquet(tablePath.toString());
    boolean exists = context.index().exists().parquet(tablePath.toString());
    assertEquals(exists, true);
  }

  @Test
  public void testCreateExistsDeleteIndex() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    // create index for all available columns
    context.index().create().indexByAll().parquet(tablePath.toString());
    assertEquals(context.index().exists().parquet(tablePath.toString()), true);
    // delete index and check that it has been removed from metastore
    context.index().delete().parquet(tablePath.toString());
    assertEquals(context.index().exists().parquet(tablePath.toString()), false);
  }

  @Test
  public void testCreateQueryIndex() throws IOException {
    // set metastore location to temp directory
    spark.conf().set(METASTORE_LOCATION, new File(dir.newFolder(), "metastore").toString());
    QueryContext context = new QueryContext(spark);
    File tablePath = new File(dir.newFolder(), "table.parquet");
    sampleParquet(spark, tablePath.toString());

    context.index().create().indexByAll().parquet(tablePath.toString());
    Dataset<Row> df = context.index().parquet(tablePath.toString()).filter("col2 = 'c'");
    assertEquals(df.count(), 1);
  }
}
