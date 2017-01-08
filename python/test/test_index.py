#!/usr/bin/env python
# -*- coding: UTF-8 -*-

#
# Copyright 2016 Lightcopy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import shutil
import tempfile
import unittest
import uuid

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import lit
from lightcopy.index import Const, QueryContext

class ConstSuite(unittest.TestCase):
    def test_parquet_source(self):
        self.assertEqual(Const.PARQUET_SOURCE, 'parquet')

    def test_metastore_conf(self):
        self.assertEqual(Const.METASTORE_LOCATION, 'spark.sql.index.metastore')

class IndexSuite(unittest.TestCase):
    def tempdir(self):
        """
        Generate random temporary directory path.
        """
        path = tempfile.gettempdir()
        return os.path.join(path, 'parquet-index-test-' + str(uuid.uuid4()))

    def setUp(self):
        self.dirpath = self.tempdir()
        self.spark = SparkSession.builder \
            .master('local[*]') \
            .appName('Pyspark test') \
            .config(Const.METASTORE_LOCATION, os.path.join(self.dirpath, 'metastore')) \
            .getOrCreate()

    def tearDown(self):
        if self.spark:
            self.spark.stop()
        self.spark = None
        shutil.rmtree(self.dirpath, ignore_errors=True)

    def test_index_wrong_init(self):
        with self.assertRaises(AttributeError):
            QueryContext(None)

    def test_manager_set_source(self):
        context = QueryContext(self.spark)
        manager = context.index.format('test-format')
        self.assertEqual(manager._source, 'test-format')

    def test_manager_set_many_sources(self):
        context = QueryContext(self.spark)
        manager = context.index.format('a').format('b').format('c')
        self.assertEqual(manager._source, 'c')

    def test_manager_set_option(self):
        context = QueryContext(self.spark)
        manager = context.index.option('key1', '1').option('key2', 2).option('key3', True)
        self.assertEqual(manager._options, {'key1': '1', 'key2': '2', 'key3': 'True'})

    def test_manager_set_options_wrong(self):
        context = QueryContext(self.spark)
        with self.assertRaises(AttributeError):
            context.index.options(None)

    def test_manager_set_options(self):
        context = QueryContext(self.spark)
        manager = context.index.option('a', '1').options({'a': '2', 'b': 3, 'c': True})
        self.assertEqual(manager._options, {'a': '2', 'b': '3', 'c': 'True'})

    def test_create_command_mode(self):
        context = QueryContext(self.spark)
        cmd = context.index.create.mode('overwrite').mode('ignore')
        self.assertEqual(cmd._mode, 'ignore')

    def test_create_command_wrong_mode(self):
        context = QueryContext(self.spark)
        cmd = context.index.create.mode(None)
        error_msg = None
        try:
            cmd.parquet(None)
        except Exception as err:
            error_msg = str(err)
        self.assertTrue(error_msg is not None)
        self.assertTrue('Unsupported mode None' in error_msg)

    def test_create_command_index_by_col(self):
        context = QueryContext(self.spark)
        cmd = context.index.create.indexBy('a')
        self.assertEqual(cmd._columns, ['a'])

    def test_create_command_index_by_cols(self):
        context = QueryContext(self.spark)
        cmd = context.index.create.indexBy('a', 'b')
        self.assertEqual(cmd._columns, ['a', 'b'])

    def test_create_command_index_by_none(self):
        context = QueryContext(self.spark)
        cmd = context.index.create.indexBy()
        self.assertEqual(cmd._columns, [])

    def test_create_command_index_by_all(self):
        context = QueryContext(self.spark)
        cmd = context.index.create.indexByAll()
        self.assertEqual(cmd._columns, None)

    def test_create_index(self):
        context = QueryContext(self.spark)
        table_path = os.path.join(self.dirpath, 'table.parquet')
        self.spark.range(0, 10).withColumn('str', lit('abc')).write.parquet(table_path)
        context.index.create.indexByAll().parquet(table_path)
        self.assertTrue(context.index.exists.parquet(table_path))

    def test_create_index_cols(self):
        context = QueryContext(self.spark)
        table_path = os.path.join(self.dirpath, 'table.parquet')
        self.spark.range(0, 10).withColumn('str', lit('abc')).write.parquet(table_path)
        context.index.create.indexBy('id', 'str').parquet(table_path)
        self.assertTrue(context.index.exists.parquet(table_path))

    def test_create_index_mode(self):
        context = QueryContext(self.spark)
        table_path = os.path.join(self.dirpath, 'table.parquet')
        self.spark.range(0, 10).withColumn('str', lit('abc')).write.parquet(table_path)
        context.index.create.mode('error').indexByAll().parquet(table_path)
        context.index.create.mode('overwrite').indexByAll().parquet(table_path)
        self.assertTrue(context.index.exists.parquet(table_path))

    def test_create_delete_index(self):
        context = QueryContext(self.spark)
        table_path = os.path.join(self.dirpath, 'table.parquet')
        self.spark.range(0, 10).withColumn('str', lit('abc')).write.parquet(table_path)
        context.index.create.indexByAll().parquet(table_path)
        self.assertTrue(context.index.exists.parquet(table_path))
        context.index.delete.parquet(table_path)
        self.assertFalse(context.index.exists.parquet(table_path))

    def test_create_query_index(self):
        context = QueryContext(self.spark)
        table_path = os.path.join(self.dirpath, 'table.parquet')
        self.spark.range(0, 10).withColumn('str', lit('abc')).write.parquet(table_path)
        context.index.create.indexByAll().parquet(table_path)
        res1 = context.index.parquet(table_path).filter('id = 3').collect()
        res2 = self.spark.read.parquet(table_path).filter('id = 3').collect()
        self.assertEqual(res1, res2)

# Load test suites
def suites():
    return [
        ConstSuite,
        IndexSuite
    ]
