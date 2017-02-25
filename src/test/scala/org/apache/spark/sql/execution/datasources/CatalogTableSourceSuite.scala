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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class CatalogTableSourceSuite extends UnitTestSuite with SparkLocal with TestMetastore {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("fail to resolve non-existent table in catalog") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val err = intercept[NoSuchTableException] {
        CatalogTableSource(metastore, "abc", options = Map.empty)
      }
      assert(err.getMessage.contains("Table or view 'abc' not found in database"))
    }
  }

  test("fail if source is temporary view and not BatchedDataSourceScanExec") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val view = "range_view"
      spark.range(0, 10).createOrReplaceTempView(view)
      try {
        val err = intercept[UnsupportedOperationException] {
          CatalogTableSource(metastore, view, options = Map.empty)
        }
        assert(err.getMessage.contains("Range (0, 10, step=1"))
      } finally {
        spark.catalog.dropTempView(view)
      }
    }
  }

  test("convert Parquet catalog table into indexed source") {
    withTempDir { dir =>
      withSQLConf("spark.sql.sources.default" -> "parquet") {
        val metastore = testMetastore(spark, dir / "test")
        val tableName = "test_parquet_table"
        spark.range(0, 10).filter("id > 0").write.saveAsTable(tableName)
        try {
          val source = CatalogTableSource(metastore, tableName, options = Map("key" -> "value"))
          val indexedSource = source.asDataSource
          indexedSource.className should be ("ParquetFormat")
          indexedSource.mode should be (source.mode)
          for ((key, value) <- source.options) {
            indexedSource.options.get(key) should be (Some(value))
          }
          indexedSource.options.get("path").isDefined should be (true)
          indexedSource.catalogTable.isDefined should be (true)
        } finally {
          spark.sql(s"drop table $tableName")
        }
      }
    }
  }

  test("fail to convert JSON catalog table into indexed source") {
    // JSON source is org.apache.spark.sql.execution.RowDataSourceScanExec
    withTempDir { dir =>
      withSQLConf("spark.sql.sources.default" -> "json") {
        val metastore = testMetastore(spark, dir / "test")
        val tableName = "test_json_table"
        spark.range(0, 10).write.saveAsTable(tableName)
        try {
          val err = intercept[UnsupportedOperationException] {
            CatalogTableSource(metastore, tableName, options = Map("key" -> "value"))
          }
          err.getMessage.contains(s"Scan json ${spark.catalog.currentDatabase}.$tableName")
        } finally {
          spark.sql(s"drop table $tableName")
        }
      }
    }
  }
}
