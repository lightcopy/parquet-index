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

/** Container for column chunk metadata */
public class ChunkDescriptor {
  private final ColumnDescriptor col;
  private final ColumnChunkMetaData metadata;
  // offset in the file where chunk starts
  private final long chunkOffset;

  ChunkDescriptor(ColumnDescriptor col, ColumnChunkMetaData metadata, long chunkOffset) {
    super();
    this.col = col;
    this.metadata = metadata;
    this.chunkOffset = chunkOffset;
  }

  /**
   * Return current column descriptor.
   * @return column descriptor
   */
  public ColumnDescriptor column() {
    return this.col;
  }

  /**
   * Get column chunk metadata.
   * @return metadata
   */
  public ColumnChunkMetaData getMetadata() {
    return this.metadata;
  }

  /**
   * Get chunk offset in a file.
   * @return byte offset
   */
  public long getChunkOffset() {
    return this.chunkOffset;
  }
}
