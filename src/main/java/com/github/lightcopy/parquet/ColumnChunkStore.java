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

import java.util.List;

import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

/**
 * Extended chunk information to hold general column information and scanned data pages info. Also
 * has a reference to dictionary page, if available.
 */
public class ColumnChunkStore {
  // dictionary page for column chunk, "null" if not available
  private final DictionaryPage dictionaryPage;
  // list of pages information
  private final List<DataPageHeaderInfo> pages;
  // == column chunk metrics ==
  // start of the column data offset
  private final long firstDataPageOffset;
  // count of the values in this block of the column
  private final long valueCount;
  // statistics for this column
  private final Statistics statistics;

  /**
   * Initialize chunk info from column metadata, optional dictionary page (can be null), and list of
   * pages in chunk, see page reader on how pages are generated.
   * @param metadata column chunk metadata
   * @param dictionaryPage dictionary page, can be null
   * @param pagesInChunk non-empty list of pages in this column chunk
   * @return column chunk information
   */
  public static ColumnChunkStore get(
      ColumnChunkMetaData metadata,
      DictionaryPage dictionaryPage,
      List<DataPageHeaderInfo> pagesInChunk) {
    return new ColumnChunkStore(dictionaryPage, pagesInChunk, metadata.getFirstDataPageOffset(),
      metadata.getValueCount(), metadata.getStatistics());
  }

  private ColumnChunkStore(
      DictionaryPage dictionaryPage,
      List<DataPageHeaderInfo> pagesInChunk,
      long firstDataPageOffset,
      long valueCount,
      Statistics statistics) {
    assert firstDataPageOffset >= 0;
    // we do not expect empty column chunks
    assert valueCount > 0;
    this.dictionaryPage = dictionaryPage;
    this.pages = pagesInChunk;
    this.firstDataPageOffset = firstDataPageOffset;
    this.valueCount = valueCount;
    this.statistics = statistics;
  }

  /**
   * Whether or not dictionary page is set.
   * @return true if dictionary page is set for this column chunk
   */
  public boolean isDictionaryPageSet() {
    return this.dictionaryPage != null;
  }

  /**
   * Get column dictionary page.
   * @return dictionary page
   */
  public DictionaryPage getDictionaryPage() {
    return this.dictionaryPage;
  }
}
