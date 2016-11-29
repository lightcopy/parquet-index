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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

public class ChunkDescriptor {
  private final ColumnDescriptor col;
  private final ColumnChunkMetaData metadata;
  private final long chunkOffset;
  private final int size;

  ChunkDescriptor(ColumnDescriptor col, ColumnChunkMetaData metadata, long chunkOffset, int size) {
    super();
    this.col = col;
    this.metadata = metadata;
    this.chunkOffset = chunkOffset;
    this.size = size;
  }

  public ColumnDescriptor columnDescriptor() {
    return this.col;
  }

  public ColumnChunkMetaData getMetadata() {
    return this.metadata;
  }

  public long getChunkOffset() {
    return this.chunkOffset;
  }
}
