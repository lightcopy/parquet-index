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

import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * DataPage header information for index. Currently stores only byte offset in column chunk, so we
 * can easily jump to the page when scanning and statistics (if available).
 */
public class DataPageHeaderInfo {
  // data page offset in chunk
  private long offset;
  // hard-coded page type, either DICTIONARY_PAGE, DATA_PAGE_V1, DATA_PAGE_V2
  private PageType pageType;
  // number of values including NULLs
  private int numValues;
  // statistics, if available, only one of them will be set,
  // will have null value if not set.
  private LongStatistics longStats;
  private IntStatistics intStats;
  private BinaryStatistics binaryStats;

  DataPageHeaderInfo(long offset, PageType pageType) {
    this.offset = offset;
    this.pageType = pageType;
    // "0" value means we have not updated count, since data pages cannot be empty, which comes
    // from row group property to have positive number of records
    this.numValues = 0;
    // "null" statistics means that no statistics provided, e.g. for dictionary page
    this.longStats = null;
    this.intStats = null;
    this.binaryStats = null;
  }

  /**
   * Get page offset in column chunk.
   * @return offset
   */
  public long getOffset() {
    return this.offset;
  }

  /**
   * Get page type.
   * @return PageType enumeration value
   */
  public PageType getPageType() {
    return this.pageType;
  }

  /**
   * Get total number of values in data page, including NULLs.
   * @return number of values
   */
  public int getNumValues() {
    return this.numValues;
  }

  /**
   * Set number of values for this page.
   * @param numValues values for the page
   * @return itself
   */
  public DataPageHeaderInfo setNumValues(int numValues) {
    assert numValues > 0;
    this.numValues = numValues;
    return this;
  }

  /**
   * Get statistics for data page. Can return "null", in this case should be treated as statistics
   * are not available.
   * @return Statistics instance
   */
  public Statistics getStatistics() {
    if (this.longStats != null) return this.longStats;
    if (this.intStats != null) return this.intStats;
    if (this.binaryStats != null) return this.binaryStats;
    return null;
  }

  /**
   * Return true, if any statistics are set for page.
   * @return boolean flag
   */
  public boolean hasStatistics() {
    return this.longStats != null || this.intStats != null || this.binaryStats != null;
  }

  /**
   * Set statistics for page.
   * @param stats raw page statistics
   * @param type column type
   * @return itself
   */
  public DataPageHeaderInfo setStatistics(
      org.apache.parquet.format.Statistics statistics,
      PrimitiveTypeName type) {
    Statistics stats = Statistics.getStatsBasedOnType(type);
    if (stats != null) {
      if (statistics.isSetMax() && statistics.isSetMin()) {
        stats.setMinMaxFromBytes(statistics.min.array(), statistics.max.array());
      }
      stats.setNumNulls(statistics.null_count);
      // resolve statistics type
      if (stats instanceof LongStatistics) {
        this.longStats = (LongStatistics) stats;
      } else if (stats instanceof IntStatistics) {
        this.intStats = (IntStatistics) stats;
      } else if (stats instanceof BinaryStatistics) {
        this.binaryStats = (BinaryStatistics) stats;
      } else {
        throw new UnsupportedOperationException("Unsupported statistics " + stats.getClass());
      }
    }
    return this;
  }
}
