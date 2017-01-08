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

from types import DictType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

__all__ = ['Const', 'QueryContext', 'DataFrameIndexManager']

# Constants used through the package
class Const(object):
    PARQUET_SOURCE = 'parquet'
    METASTORE_LOCATION = 'spark.sql.index.metastore'

class CreateIndexCommand(object):
    """
    'CreateIndexCommand' provides functionality to create index for a table. Requires index
    columns and valid table path. Also allows to specify different mode for creating index
    (org.apache.spark.sql.SaveMode) or its string representation similar to 'DataFrame.write'.
    """
    def __init__(self, manager):
        self._manager = manager
        self._mode = None
        self._columns = None

    def mode(self, value):
        """
        Set mode to create table. Several modes are available:
        - 'append' append data to existing index, or create new one if dost not exist
        - 'overwrite' delete index if one already exists for the table, and create it
        - 'error' create index, raise an error if index already exists
        - 'ignore' create index, and no-op if index already exists
        Default mode is 'error', which is set in Scala code.

        :param value: mode value (see possible options above)
        :return: self
        """
        self._mode = '%s' % (value)
        return self

    def indexBy(self, *columns):
        """
        Set column or list of columns to use when building index. Statistics will be collected for
        these columns and optionally filter statistics.

        :param *columns: args of column names
        :return: self
        """
        self._columns = ['%s' % (column) for column in columns]
        return self

    def indexByAll(self):
        """
        Set columns to None, which results in inferring all columns from further provided table
        that can be used for indexing and creating index using those columns.

        :return: self
        """
        self._columns = None
        return self

    def table(self, path):
        """
        Create index for table path.

        :param path: path to the table
        """
        jcreate = self._manager._jdim.create()
        # set mode if available, otherwise will use default value in Scala code
        if self._mode:
            jcreate.mode(self._mode)
        # set columns, if columns is None, infer all possible columns from table using
        # 'indexByAll', otherwise use provided list of column names and invoke 'indexBy'
        if self._columns is None:
            jcreate.indexByAll()
        else:
            jcreate.indexBy(self._columns)
        jcreate.table(path)

    def parquet(self, path):
        """
        Create index for Parquet table. Forces 'parquet' format and overwrites any other defined
        previously.

        :param path: path to the Parquet table
        """
        self._manager.format(Const.PARQUET_SOURCE)
        self.table(path)

class ExistsIndexCommand(object):
    """
    'ExistsIndexCommand' reports whether or not given table path is indexed.
    """
    def __init__(self, manager):
        self._manager = manager

    def table(self, path):
        """
        Load index from metastore for the table path and check its existence. Uses provided source
        from 'DataFrameIndexManager'.

        :param path: path to the table
        :return: True if index exists, False otherwise
        """
        return self._manager._jdim.exists().table(path)

    def parquet(self, path):
        """
        Load index for Parquet table from metastore and check its existence. Forces 'parquet'
        source and overwrites any other source set before.

        :param path: path to the Parquet table
        :return: True if index exists, False otherwise
        """
        self._manager.format(Const.PARQUET_SOURCE)
        return self.table(path)

class DeleteIndexCommand(object):
    """
    'DeleteIndexCommand' provides functionality to delete existing index. Current behaviour is
    no-op when deleting non-existent index.
    """
    def __init__(self, manager):
        self._manager = manager

    def table(self, path):
        """
        Delete index from metastore for table path. If index does not exist, results in no-op.
        Uses provided source from 'DataFrameIndexManager'.

        :param path: path to the table
        """
        self._manager._jdim.delete().table(path)

    def parquet(self, path):
        """
        Delete index from metastore for Parquet table. If index does not exist, results in no-op.
        Forces 'parquet' source and overwrites any other source set before.

        :param path: path to the Parquet table
        """
        self._manager.format(Const.PARQUET_SOURCE)
        self.table(path)

class DataFrameIndexManager(object):
    """
    Entrypoint for working with index functionality, e.g. reading indexed table, creating index
    for provided file path, or deleting index for table.
    See examples on usage:

    >>> df = context.index.format("parquet").option("key", "value").load("path")
    >>> df = context.index.parquet("path")
    >>> context.index.exists.parquet("path")
    True
    >>> context.index.delete.parquet("path")
    """
    def __init__(self, context):
        if not isinstance(context, QueryContext):
            msg = 'Expected <QueryContext> instance, found %s' % (context)
            raise AttributeError(msg)
        # SQLContext to create DataFrame
        self._sqlctx = context.spark_session._wrapped
        # manager for context
        self._jdim = context._jcontext.index()
        # data source that matches table type, by default is 'parquet'
        self._source = Const.PARQUET_SOURCE
        # extra options to use for data source
        self._options = {}

    def _init_manager(self):
        """
        Initialize java index manager by setting format and all options that were set before
        the call.
        """
        self._jdim.format(self._source)
        for (key, value) in self._options.items():
            self._jdim.option(key, value)

    def format(self, source):
        """
        Set file format for table, e.g. 'parquet'.

        :param source: source to set
        :return: self
        """
        self._source = '%s' % (source)
        return self

    def option(self, key, value):
        """
        Add new option for this manager, if option key is already in options, will overwrite
        existing value. All options are stored as strings. This method is added mainly for
        parity with 'DataFrameReader', therefore all options will be applied the same way.

        :param key: option key
        :param value: option value
        :return: self
        """
        str_key = '%s' % (key)
        str_value = '%s' % (value)
        self._options[str_key] = str_value
        return self

    def options(self, opts):
        """
        Add multiple options as dictionary. Raises error if passed instance is not a <dict>.
        These options will overwrite already set ones.

        :param: opts dictionary
        :return: self
        """
        if not isinstance(opts, DictType):
            msg = 'Expected <dict>, found %s' % (opts)
            raise AttributeError(msg)
        for (key, value) in opts.items():
            self.option(key, value)
        return self

    def parquet(self, path):
        """
        Shortcut for setting source as 'parquet' and loading DataFrame.
        Recommended to use over standard interface (format -> load).

        :param path: path to the Parquet table
        :return: DataFrame instance
        """
        self.format(Const.PARQUET_SOURCE)
        return self.load(path)

    def load(self, path):
        """
        Load table path and return DataFrame for provided source and options.

        :param path: path to the table
        :return: DataFrame instance
        """
        self._init_manager()
        return DataFrame(self._jdim.load(path), self._sqlctx)

    @property
    def create(self):
        """
        Access to 'create index' functionality.

        :return: CreateIndexCommand instance
        """
        self._init_manager()
        return CreateIndexCommand(self)

    @property
    def exists(self):
        """
        Access to 'exists index' functionality.

        :return: ExistsIndexCommand instance
        """
        self._init_manager()
        return ExistsIndexCommand(self)

    @property
    def delete(self):
        """
        Access to 'delete index' functionality.

        :return: DeleteIndexCommand instance
        """
        self._init_manager()
        return DeleteIndexCommand(self)

class QueryContext(object):
    """
    The entrypoint to programming Spark with index functionality.

    QueryContext can be used to create 'DataFrame' with index support by creating filesystem
    metastore and storing index information there, which includes min/max/nulls statistics and
    optional filter statistics, e.g. bloom filters for selected indexed columns.
    To create QueryContext use example:

    >>> from pyspark.sql.session import SparkSession
    >>> spark = SparkSession.builder...
    >>> from lightcopy.index import QueryContext
    >>> context = QueryContext(spark)
    """
    def __init__(self, session):
        if not isinstance(session, SparkSession):
            msg = 'Expected <SparkSession> to initialize query context, found %s' % (session)
            raise AttributeError(msg)
        _jvm = session.sparkContext._jvm
        self._spark_session = session
        self._jcontext = _jvm.com.github.lightcopy.QueryContext(session._jsparkSession)

    @property
    def spark_session(self):
        """
        Return reference to SparkSession instance used to create this QueryContext.

        :return: SparkSession instance
        """
        return self._spark_session

    @property
    def index(self):
        """
        Return DataFrameIndexManager instance to get access to index functionality.
        This is the main method to query DataFrame or create index.

        :return: DataFrameIndexManager instance
        """
        return DataFrameIndexManager(self)
