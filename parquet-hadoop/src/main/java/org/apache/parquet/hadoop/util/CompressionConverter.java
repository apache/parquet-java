/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.util;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CompressionConverter {

  private static final Logger LOG = LoggerFactory.getLogger(CompressionConverter.class);

  private final int pageBufferSize = ParquetProperties.DEFAULT_PAGE_SIZE * 2;
  private byte[] pageBuffer;

  public CompressionConverter() {
    this.pageBuffer = new byte[pageBufferSize];
  }

  public void processBlocks(TransParquetFileReader reader, ParquetFileWriter writer, ParquetMetadata meta, MessageType schema,
                            String createdBy, CompressionCodecName codecName) throws IOException {
    int blockIndex = 0;
    PageReadStore store = reader.readNextRowGroup();
    while (store != null) {
      writer.startBlock(store.getRowCount());
      BlockMetaData blockMetaData = meta.getBlocks().get(blockIndex);
      List<ColumnChunkMetaData> columnsInOrder = blockMetaData.getColumns();
      Map<ColumnPath, ColumnDescriptor> descriptorsMap = schema.getColumns().stream().collect(
        Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
      for (int i = 0; i < columnsInOrder.size(); i += 1) {
        ColumnChunkMetaData chunk = columnsInOrder.get(i);
        ColumnReadStoreImpl crstore = new ColumnReadStoreImpl(store, new DummyGroupConverter(), schema, createdBy);
        ColumnDescriptor columnDescriptor = descriptorsMap.get(chunk.getPath());
        writer.startColumn(columnDescriptor, crstore.getColumnReader(columnDescriptor).getTotalValueCount(), codecName);
        processChunk(reader, writer, chunk, createdBy, codecName);
        writer.endColumn();
      }
      writer.endBlock();
      store = reader.readNextRowGroup();
      blockIndex++;
    }
  }

  private void processChunk(TransParquetFileReader reader, ParquetFileWriter writer, ColumnChunkMetaData chunk,
                            String createdBy, CompressionCodecName codecName) throws IOException {
    CompressionCodecFactory codecFactory = HadoopCodecs.newFactory(0);
    CompressionCodecFactory.BytesInputDecompressor decompressor = codecFactory.getDecompressor(chunk.getCodec());
    CompressionCodecFactory.BytesInputCompressor compressor = codecFactory.getCompressor(codecName);
    ColumnIndex columnIndex = reader.readColumnIndex(chunk);
    OffsetIndex offsetIndex = reader.readOffsetIndex(chunk);

    reader.setStreamPosition(chunk.getStartingPos());
    DictionaryPage dictionaryPage = null;
    long readValues = 0;
    Statistics statistics = null;
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    int pageIndex = 0;
    long totalChunkValues = chunk.getValueCount();
    while (readValues < totalChunkValues) {
      PageHeader pageHeader = reader.readPageHeader();
      int compressedPageSize = pageHeader.getCompressed_page_size();
      byte[] pageLoad;
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          if (dictionaryPage != null) {
            throw new IOException("has more than one dictionary page in column chunk");
          }
          DictionaryPageHeader dictPageHeader = pageHeader.dictionary_page_header;
          pageLoad = translatePageLoad(reader, true, compressor, decompressor, pageHeader.getCompressed_page_size(), pageHeader.getUncompressed_page_size());
          dictionaryPage = new DictionaryPage(BytesInput.from(pageLoad),
                                                   pageHeader.getUncompressed_page_size(),
                                                   dictPageHeader.getNum_values(),
                                                   converter.getEncoding(dictPageHeader.getEncoding()));
          writer.writeDictionaryPage(dictionaryPage);
          break;
        case DATA_PAGE:
          DataPageHeader headerV1 = pageHeader.data_page_header;
          pageLoad = translatePageLoad(reader, true, compressor, decompressor, pageHeader.getCompressed_page_size(), pageHeader.getUncompressed_page_size());
          statistics = convertStatistics(createdBy, chunk.getPrimitiveType(), headerV1.getStatistics(), columnIndex, pageIndex, converter);
          readValues += headerV1.getNum_values();
          if (offsetIndex != null) {
            long rowCount = 1 + offsetIndex.getLastRowIndex(pageIndex, totalChunkValues) - offsetIndex.getFirstRowIndex(pageIndex);
            writer.writeDataPage(toIntWithCheck(headerV1.getNum_values()),
              pageHeader.getUncompressed_page_size(),
              BytesInput.from(pageLoad),
              statistics,
              toIntWithCheck(rowCount),
              converter.getEncoding(headerV1.getRepetition_level_encoding()),
              converter.getEncoding(headerV1.getDefinition_level_encoding()),
              converter.getEncoding(headerV1.getEncoding()));
          } else {
            writer.writeDataPage(toIntWithCheck(headerV1.getNum_values()),
              pageHeader.getUncompressed_page_size(),
              BytesInput.from(pageLoad),
              statistics,
              converter.getEncoding(headerV1.getRepetition_level_encoding()),
              converter.getEncoding(headerV1.getDefinition_level_encoding()),
              converter.getEncoding(headerV1.getEncoding()));
          }
          pageIndex++;
          break;
        case DATA_PAGE_V2:
          DataPageHeaderV2 headerV2 = pageHeader.data_page_header_v2;
          int rlLength = headerV2.getRepetition_levels_byte_length();
          BytesInput rlLevels = readBlockAllocate(rlLength, reader);
          int dlLength = headerV2.getDefinition_levels_byte_length();
          BytesInput dlLevels = readBlockAllocate(dlLength, reader);
          int payLoadLength = pageHeader.getCompressed_page_size() - rlLength - dlLength;
          int rawDataLength = pageHeader.getUncompressed_page_size() - rlLength - dlLength;
          pageLoad = translatePageLoad(reader, headerV2.is_compressed, compressor, decompressor, payLoadLength, rawDataLength);
          statistics = convertStatistics(createdBy, chunk.getPrimitiveType(), headerV2.getStatistics(), columnIndex, pageIndex, converter);
          readValues += headerV2.getNum_values();
          writer.writeDataPageV2(headerV2.getNum_rows(),
            headerV2.getNum_nulls(),
            headerV2.getNum_values(),
            rlLevels,
            dlLevels,
            converter.getEncoding(headerV2.getEncoding()),
            BytesInput.from(pageLoad),
            rawDataLength,
            statistics);
          pageIndex++;
          break;
        default:
          LOG.debug("skipping page of type {} of size {}", pageHeader.getType(), compressedPageSize);
          break;
      }
    }
  }

  private Statistics convertStatistics(String createdBy, PrimitiveType type, org.apache.parquet.format.Statistics pageStatistics,
                                       ColumnIndex columnIndex, int pageIndex, ParquetMetadataConverter converter) throws IOException {
    if (columnIndex != null) {
      if (columnIndex.getNullPages() == null) {
        throw new IOException("columnIndex has null variable 'nullPages' which indicates corrupted data for type: " +  type.getName());
      }
      if (pageIndex > columnIndex.getNullPages().size()) {
        throw new IOException("There are more pages " + pageIndex + " found in the column than in the columnIndex " + columnIndex.getNullPages().size());
      }
      org.apache.parquet.column.statistics.Statistics.Builder statsBuilder = org.apache.parquet.column.statistics.Statistics.getBuilderForReading(type);
      statsBuilder.withNumNulls(columnIndex.getNullCounts().get(pageIndex));

      if (!columnIndex.getNullPages().get(pageIndex)) {
        statsBuilder.withMin(columnIndex.getMinValues().get(pageIndex).array().clone());
        statsBuilder.withMax(columnIndex.getMaxValues().get(pageIndex).array().clone());
      }
      return statsBuilder.build();
    } else if (pageStatistics != null) {
      return converter.fromParquetStatistics(createdBy, pageStatistics, type);
    } else {
      return null;
    }
  }

  private byte[] translatePageLoad(TransParquetFileReader reader, boolean isCompressed, CompressionCodecFactory.BytesInputCompressor compressor,
                                   CompressionCodecFactory.BytesInputDecompressor decompressor, int payloadLength, int rawDataLength) throws IOException {
    BytesInput data = readBlock(payloadLength, reader);
    if (isCompressed) {
      data = decompressor.decompress(data, rawDataLength);
    }
    BytesInput newCompressedData = compressor.compress(data);
    return newCompressedData.toByteArray();
  }

  public BytesInput readBlock(int length, TransParquetFileReader reader) throws IOException {
    byte[] data;
    if (length > pageBufferSize) {
      data = new byte[length];
    } else {
      data = pageBuffer;
    }
    reader.blockRead(data, 0, length);
    return BytesInput.from(data, 0, length);
  }

  public BytesInput readBlockAllocate(int length, TransParquetFileReader reader) throws IOException {
    byte[] data = new byte[length];
    reader.blockRead(data, 0, length);
    return BytesInput.from(data, 0, length);
  }

  private int toIntWithCheck(long size) {
    if ((int)size != size) {
      throw new ParquetEncodingException("size is bigger than " + Integer.MAX_VALUE + " bytes: " + size);
    }
    return (int)size;
  }

  private static final class DummyGroupConverter extends GroupConverter {
    @Override public void start() {}
    @Override public void end() {}
    @Override public Converter getConverter(int fieldIndex) { return new DummyConverter(); }
  }

  private static final class DummyConverter extends PrimitiveConverter {
    @Override public GroupConverter asGroupConverter() { return new DummyGroupConverter(); }
  }

  public static final class TransParquetFileReader extends ParquetFileReader {

    public TransParquetFileReader(InputFile file, ParquetReadOptions options) throws IOException {
      super(file, options);
    }

    public void setStreamPosition(long newPos) throws IOException {
      f.seek(newPos);
    }

    public void blockRead(byte[] data, int start, int len) throws IOException {
      f.readFully(data, start, len);
    }

    public PageHeader readPageHeader() throws IOException {
      return Util.readPageHeader(f);
    }

    public PageHeader readPageHeader(BlockCipher.Decryptor decryptor, byte[] AAD) throws IOException {
      return Util.readPageHeader(f, decryptor, AAD);
    }

    public long getPos() throws IOException {
      return f.getPos();
    }

    public SeekableInputStream getStream() {
      return f;
    }
  }
}
