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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.nio.ByteBuffer;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;

/**
 * Column chunk reader is a copy of Parquet Chunk (part of ParquetFileReader ) with additional
 * functionality of indexing data pages. Unfortunately class is private in parquet, so most of the
 * methods are duplicated here.
 */
public class ColumnChunkReader extends ByteArrayInputStream {
  // column chunk descriptor
  private final ChunkDescriptor descriptor;
  // Parquet metadata converter
  private final ParquetMetadataConverter converter;

  public ColumnChunkReader(
      ChunkDescriptor descriptor,
      byte[] data,
      int offset) {
    this(descriptor, data, offset, new ParquetMetadataConverter());
  }

  /**
   * Initialize column chunk reader.
   * @param descriptor descriptor for the chunk
   * @param data contains the chunk data at offset
   * @param offset where the chunk starts in offset
   * @param metadata converter
   */
  public ColumnChunkReader(
      ChunkDescriptor descriptor,
      byte[] data,
      int offset,
      ParquetMetadataConverter converter) {
    super(data);
    assert offset >= 0;
    this.descriptor = descriptor;
    this.converter = converter;
    this.pos = offset;
  }

  /** Read page header, copied from Parquet */
  protected PageHeader readPageHeader() throws IOException {
    return Util.readPageHeader(this);
  }

  /** Get current position in a stream */
  protected int pos() {
    return this.pos;
  }

  /** Read DataPage from current offset up to size */
  public BytesInput readAsBytesInput(int size) throws IOException {
    return readAsBytesInput(pos(), size);
  }

  /** Read DataPage data from position up to size */
  public BytesInput readAsBytesInput(int pos, int size) throws IOException {
    final BytesInput r = BytesInput.from(this.buf, pos, size);
    // always increment position as provided position + size
    this.pos = pos + size;
    return r;
  }

  /**
   * Read all pages' headers and return statistics information only without reading data, the only
   * exception is dictionary page, where we read entire page to use later for indexing, if possible.
   * @return column metadata with pages information
   */
  public ColumnChunkStore readAllPageInfos() throws IOException {
    List<DataPageHeaderInfo> pagesInChunk = new ArrayList<DataPageHeaderInfo>();
    long valuesCountReadSoFar = 0;
    long metadataValues = descriptor.getMetadata().getValueCount();
    DictionaryPage dictionaryPage = null;
    while (valuesCountReadSoFar < metadataValues) {
      PageHeader pageHeader = readPageHeader();
      int uncompressedPageSize = pageHeader.getUncompressed_page_size();
      int compressedPageSize = pageHeader.getCompressed_page_size();
      DataPageHeaderInfo info = null;
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          // there is only one dictionary page per column chunk
          // in this case we actually read page and return it
          if (dictionaryPage != null) {
            throw new IllegalStateException("More than one dictionary page in column " +
              descriptor.column());
          }
          DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
          dictionaryPage = new DictionaryPage(
            readAsBytesInput(compressedPageSize),
            uncompressedPageSize,
            dicHeader.getNum_values(),
            converter.getEncoding(dicHeader.getEncoding()));
          break;
        case DATA_PAGE:
          DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
          info = new DataPageHeaderInfo(this.pos(), PageType.DATA_PAGE_V1).
            setNumValues(dataHeaderV1.getNum_values()).
            setStatistics(dataHeaderV1.getStatistics(), descriptor.column().getType());
          pagesInChunk.add(info);
          valuesCountReadSoFar += dataHeaderV1.getNum_values();
          this.skip(compressedPageSize);
          break;
        case DATA_PAGE_V2:
          DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
          info = new DataPageHeaderInfo(this.pos(), PageType.DATA_PAGE_V2).
            setNumValues(dataHeaderV2.getNum_values()).
            setStatistics(dataHeaderV2.getStatistics(), descriptor.column().getType());
          pagesInChunk.add(info);
          valuesCountReadSoFar += dataHeaderV2.getNum_values();
          this.skip(compressedPageSize);
          break;
        default:
          info = null;
          this.skip(compressedPageSize);
          break;
      }
    }

    if (valuesCountReadSoFar != metadataValues) {
      throw new IOException(
        "Expected " + descriptor.getMetadata().getValueCount() +
        " values in column chunk at offset " + descriptor.getMetadata().getFirstDataPageOffset() +
        " but got " + valuesCountReadSoFar + " values instead over " + pagesInChunk.size() +
        " pages ending at file offset " + (descriptor.getChunkOffset() + pos()));
    }

    return ColumnChunkStore.get(descriptor.getMetadata(), dictionaryPage, pagesInChunk);
  }
}
