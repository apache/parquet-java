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
package parquet.hadoop;

import static parquet.Log.INFO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parquet.Log;
import parquet.bytes.ByteBufferAllocator;
import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageWriteStore;
import parquet.column.page.PageWriter;
import parquet.column.statistics.Statistics;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactory.BytesCompressor;
import parquet.io.ParquetEncodingException;
import parquet.schema.MessageType;

class ColumnChunkPageWriteStore implements PageWriteStore {
  private static final Log LOG = Log.getLog(ColumnChunkPageWriteStore.class);

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private static final class ColumnChunkPageWriter implements PageWriter {

    private final ColumnDescriptor path;
    private final BytesCompressor compressor;

    private final CapacityByteArrayOutputStream buf;
    private DictionaryPage dictionaryPage;

    private long uncompressedLength;
    private long compressedLength;
    private long totalValueCount;
    private int pageCount;

    private Set<Encoding> encodings = new HashSet<Encoding>();

    private Statistics totalStatistics;
    private ByteBufferAllocator allocator;

    private ColumnChunkPageWriter(ColumnDescriptor path,
                                  BytesCompressor compressor,
                                  int initialSize,
                                  ByteBufferAllocator allocator) {
      this.path = path;
      this.compressor = compressor;
      this.allocator=allocator;
      this.buf = new CapacityByteArrayOutputStream(initialSize, allocator);
      this.totalStatistics = Statistics.getStatsBasedOnType(this.path.getType());
    }

    @Override
    public void writePage(BytesInput bytes,
                          int valueCount,
                          Statistics statistics,
                          Encoding rlEncoding,
                          Encoding dlEncoding,
                          Encoding valuesEncoding) throws IOException {
      long uncompressedSize = bytes.size();
      if (uncompressedSize > Integer.MAX_VALUE) {
        throw new ParquetEncodingException(
            "Cannot write page larger than Integer.MAX_VALUE bytes: " +
            uncompressedSize);
      }
      BytesInput compressedBytes = compressor.compress(bytes);
      long compressedSize = compressedBytes.size();
      if (compressedSize > Integer.MAX_VALUE) {
        throw new ParquetEncodingException(
            "Cannot write compressed page larger than Integer.MAX_VALUE bytes: "
            + compressedSize);
      }
      parquetMetadataConverter.writeDataPageHeader(
          (int)uncompressedSize,
          (int)compressedSize,
          valueCount,
          statistics,
          rlEncoding,
          dlEncoding,
          valuesEncoding,
          buf);
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += valueCount;
      this.pageCount += 1;
      this.totalStatistics.mergeStatistics(statistics);
      compressedBytes.writeAllTo(buf);
      encodings.add(rlEncoding);
      encodings.add(dlEncoding);
      encodings.add(valuesEncoding);
    }

    @Override
    public void writePageV2(
        int rowCount, int nullCount, int valueCount,
        BytesInput repetitionLevels, BytesInput definitionLevels,
        Encoding dataEncoding, BytesInput data,
        Statistics<?> statistics) throws IOException {
      int rlByteLength = toIntWithCheck(repetitionLevels.size());
      int dlByteLength = toIntWithCheck(definitionLevels.size());
      int uncompressedSize = toIntWithCheck(
          data.size() + repetitionLevels.size() + definitionLevels.size()
      );
      // TODO: decide if we compress
      BytesInput compressedData = compressor.compress(data);
      int compressedSize = toIntWithCheck(
          compressedData.size() + repetitionLevels.size() + definitionLevels.size()
      );
      parquetMetadataConverter.writeDataPageV2Header(
          uncompressedSize, compressedSize,
          valueCount, nullCount, rowCount,
          statistics,
          dataEncoding,
          rlByteLength, dlByteLength,
          buf);
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += valueCount;
      this.pageCount += 1;
      this.totalStatistics.mergeStatistics(statistics);
      repetitionLevels.writeAllTo(buf);
      definitionLevels.writeAllTo(buf);
      compressedData.writeAllTo(buf);
      encodings.add(dataEncoding);
    }

    private int toIntWithCheck(long size) {
      if (size > Integer.MAX_VALUE) {
        throw new ParquetEncodingException(
            "Cannot write page larger than " + Integer.MAX_VALUE + " bytes: " +
            size);
      }
      return (int)size;
    }

    @Override
    public long getMemSize() {
      return buf.size();
    }

    public void writeToFileWriter(ParquetFileWriter writer) throws IOException {
      writer.startColumn(path, totalValueCount, compressor.getCodecName());
      if (dictionaryPage != null) {
        writer.writeDictionaryPage(dictionaryPage);
        encodings.add(dictionaryPage.getEncoding());
      }
      writer.writeDataPages(BytesInput.from(buf), uncompressedLength, compressedLength, totalStatistics, new ArrayList<Encoding>(encodings));
      writer.endColumn();
      if (INFO) {
        LOG.info(
            String.format(
                "written %,dB for %s: %,d values, %,dB raw, %,dB comp, %d pages, encodings: %s",
                buf.size(), path, totalValueCount, uncompressedLength, compressedLength, pageCount, encodings)
            + (dictionaryPage != null ? String.format(
                    ", dic { %,d entries, %,dB raw, %,dB comp}",
                    dictionaryPage.getDictionarySize(), dictionaryPage.getUncompressedSize(), dictionaryPage.getDictionarySize())
                    : ""));
      }
      encodings.clear();
      pageCount = 0;
    }

    @Override
    public long allocatedSize() {
      return buf.getCapacity();
    }

    @Override
    public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
      if (this.dictionaryPage != null) {
        throw new ParquetEncodingException("Only one dictionary page is allowed");
      }
      BytesInput dictionaryBytes = dictionaryPage.getBytes();
      int uncompressedSize = (int)dictionaryBytes.size();
      BytesInput compressedBytes = compressor.compress(dictionaryBytes);
      this.dictionaryPage = new DictionaryPage(BytesInput.copy(compressedBytes), uncompressedSize, dictionaryPage.getDictionarySize(), dictionaryPage.getEncoding());
    }

    @Override
    public String memUsageString(String prefix) {
      return buf.memUsageString(prefix + " ColumnChunkPageWriter");
    }

    @Override
    public ByteBufferAllocator getAllocator() {
      return this.allocator;
    }

    @Override
    public void reset(){
      this.buf.reset();
    }

    @Override
    public void close(){
      this.buf.close();
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkPageWriter> writers = new HashMap<ColumnDescriptor, ColumnChunkPageWriter>();
  private final MessageType schema;
  private final BytesCompressor compressor;
  private final int initialSize;
  private ByteBufferAllocator allocator;

  public ColumnChunkPageWriteStore(BytesCompressor compressor, MessageType schema, int initialSize, ByteBufferAllocator allocator) {
    for (ColumnDescriptor path : schema.getColumns()) {
      writers.put(path,  new ColumnChunkPageWriter(path, compressor, initialSize, allocator));
    }
    this.compressor = compressor;
    this.schema = schema;
    this.initialSize = initialSize;
    this.allocator=allocator;
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    if (!writers.containsKey(path)) {
      writers.put(path,  new ColumnChunkPageWriter(path, compressor, initialSize, this.allocator));
    }
    return writers.get(path);
  }

  public void flushToFileWriter(ParquetFileWriter writer) throws IOException {
    for (ColumnDescriptor path : schema.getColumns()) {
      ColumnChunkPageWriter pageWriter = writers.get(path);
      pageWriter.writeToFileWriter(writer);
      pageWriter.reset();
    }
  }

  public void close() throws IOException {
    List<ColumnDescriptor> columns = schema.getColumns();
    for (ColumnDescriptor columnDescriptor : columns) {
      ColumnChunkPageWriter pageWriter = writers.get(columnDescriptor);
      pageWriter.close();
      writers.remove(pageWriter);
    }
  }

}
