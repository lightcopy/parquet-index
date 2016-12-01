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

import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.column.ColumnDescriptor;

/**
 * Container to keep different sets of metadata:
 * - BlockMetadata:
 *  - ColumnChunkMetaData:
 *   - Page 1 information
 *   - Page 2 information
 * ...
 * We do not store entire metadata, only selected fields, e.g. total row count, column descriptor,
 * including type, column chunk statistics, and information about data pages in chunk.
 */
public class BlockMetadataStore {
  private final Map<ColumnDescriptor, ColumnChunkStore> columns;
  // number of rows in this block
  private final long rowCount;
  // starting position of the first column in block
  private final long startingPos;

  BlockMetadataStore(long rowCount, long startingPos) {
    this.rowCount = rowCount;
    this.startingPos = startingPos;
    this.columns = new HashMap<ColumnDescriptor, ColumnChunkStore>();
  }

  public void addColumn(ColumnDescriptor path, ColumnChunkStore columnInfo) {
    if (this.columns.put(path, columnInfo) != null) {
      throw new RuntimeException("Path " + path + "was added twice");
    }
  }
}
