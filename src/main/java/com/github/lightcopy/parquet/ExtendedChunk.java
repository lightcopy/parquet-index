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
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;

public class ExtendedChunk extends ByteArrayInputStream {
  private final ChunkDescriptor descriptor;
  private final ParquetMetadataConverter converter;

  public ExtendedChunk(
      ChunkDescriptor descriptor,
      byte[] data,
      int offset,
      ParquetMetadataConverter converter) {
    super(data);
    this.descriptor = descriptor;
    this.converter = converter;
    this.pos = offset;
  }

  protected PageHeader readPageHeader() throws IOException {
    return Util.readPageHeader(this);
  }

  protected int pos() {
    return this.pos;
  }

  protected void incrementPos(int offset) {
    this.pos += offset;
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

  public List<DataPageHeaderInfo> readAllPageInfos() throws IOException {
    List<DataPageHeaderInfo> pagesInChunk = new ArrayList<DataPageHeaderInfo>();
    long valuesCountReadSoFar = 0;
    long metadataValues = descriptor.getMetadata().getValueCount();
    boolean foundDictionaryPage = false;
    while (valuesCountReadSoFar < metadataValues) {
      PageHeader pageHeader = readPageHeader();
      int uncompressedPageSize = pageHeader.getUncompressed_page_size();
      int compressedPageSize = pageHeader.getCompressed_page_size();
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          // there is only one dictionary page per column chunk
          if (foundDictionaryPage) {
            throw new IllegalStateException("More than one dictionary page in column " +
              descriptor.columnDescriptor());
          }
          DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
          pagesInChunk.add(new DataPageHeaderInfo(
            this.pos(),
            "DICTIONARY_PAGE",
            dicHeader.getNum_values(),
            null,
            null,
            converter.getEncoding(dicHeader.getEncoding()).name(),
            null
          ));
          this.skip(compressedPageSize);
          break;
        case DATA_PAGE:
          DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
          pagesInChunk.add(new DataPageHeaderInfo(
            this.pos(),
            "DATA_PAGE_V1",
            dataHeaderV1.getNum_values(),
            converter.getEncoding(dataHeaderV1.getRepetition_level_encoding()).name(),
            converter.getEncoding(dataHeaderV1.getDefinition_level_encoding()).name(),
            converter.getEncoding(dataHeaderV1.getEncoding()).name(),
            ParquetMetadataConverter.fromParquetStatistics(
              dataHeaderV1.getStatistics(), descriptor.columnDescriptor().getType())
          ));
          valuesCountReadSoFar += dataHeaderV1.getNum_values();
          this.skip(compressedPageSize);
          break;
        case DATA_PAGE_V2:
          DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
          int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() -
            dataHeaderV2.getDefinition_levels_byte_length();
          pagesInChunk.add(new DataPageHeaderInfo(
            this.pos(),
            "DATA_PAGE_V2",
            dataHeaderV2.getNum_values(),
            null,
            null,
            converter.getEncoding(dataHeaderV2.getEncoding()).name(),
            ParquetMetadataConverter.fromParquetStatistics(
              dataHeaderV2.getStatistics(), descriptor.columnDescriptor().getType())
          ));
          valuesCountReadSoFar += dataHeaderV2.getNum_values();
          this.skip(compressedPageSize);
          break;
        default:
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

    return pagesInChunk;
  }
}
