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

import org.apache.parquet.column.statistics.Statistics;

/** DataPage header information restricted to have only statistics and offset */
public class DataPageHeaderInfo {
  // data page offset in chunk
  private long offset;
  // hard-coded page type, either DICTIONARY_PAGE, DATA_PAGE_V1, DATA_PAGE_V2
  private String pageType;
  // number of values including NULLs
  private int numValues;
  // repetition level encoding
  private String rlEncoding;
  // definition level encoding
  private String dlEncoding;
  // value encoding for this page
  private String valueEncoding;
  private Statistics statistics;

  DataPageHeaderInfo(
      long offset,
      String pageType,
      int numValues,
      String rlEncoding,
      String dlEncoding,
      String valueEncoding,
      Statistics statistics) {
    this.offset = offset;
    this.numValues = numValues;
    this.rlEncoding = rlEncoding;
    this.dlEncoding = dlEncoding;
    this.valueEncoding = valueEncoding;
    this.statistics = statistics;
  }

  public long getOffset() {
    return this.offset;
  }

  public String getPageType() {
    return this.pageType;
  }

  public int getNumValues() {
    return this.numValues;
  }

  public String getRlEncoding() {
    return this.rlEncoding;
  }

  public String getDlEncoding() {
    return this.dlEncoding;
  }

  public Statistics getStatistics() {
    return this.statistics;
  }
}
