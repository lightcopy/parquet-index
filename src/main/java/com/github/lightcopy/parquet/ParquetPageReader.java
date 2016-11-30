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

package com.github.lightcopy.parquet;

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

/**
 * Internal reader to load data pages from a file, has similar meaning as ParquetFileReader class
 * in Parquet repository, but this is simplified version.
 */
public class ParquetPageReader {

  private final Path filePath;
  private final FSDataInputStream stream;
  private final List<BlockMetaData> blocks;
  private final Map<ColumnPath, ColumnDescriptor> columns;
  private int currentBlock = 0;

  public ParquetPageReader(
      Configuration configuration,
      Path filePath,
      List<BlockMetaData> blocks,
      List<ColumnDescriptor> columns) throws IOException {
    this.filePath = filePath;
    this.blocks = blocks;
    this.columns = new HashMap<ColumnPath, ColumnDescriptor>();
    for (ColumnDescriptor col : columns) {
      this.columns.put(ColumnPath.get(col.getPath()), col);
    }
    FileSystem fs = filePath.getFileSystem(configuration);
    this.stream = fs.open(filePath);
  }

  /** Release associated resources */
  public void close() {
    try {
      this.stream.close();
    } catch (IOException err) {
      // do nothing
    }
  }

  /** Read next row group if available, and collect information about data pages */
  public Map<ColumnDescriptor, ExtendedChunkInfo> readNextRowGroup() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    BlockMetaData block = blocks.get(currentBlock);
    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }

    Map<ColumnDescriptor, ExtendedChunkInfo> rowGroup =
      new HashMap<ColumnDescriptor, ExtendedChunkInfo>();
    return rowGroup;
  }
}
