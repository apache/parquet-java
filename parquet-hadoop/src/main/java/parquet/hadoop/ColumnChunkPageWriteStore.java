/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop;

import static parquet.Log.INFO;
import static parquet.column.statistics.Statistics.getStatsBasedOnType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.bytes.ConcatenatingByteArrayCollector;
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
  private static final int COLUMN_CHUNK_WRITER_MAX_SIZE_HINT = 64 * 1024;

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private static final class ColumnChunkPageWriter implements PageWriter {

    private final ColumnDescriptor path;
    private final BytesCompressor compressor;

    private final ByteArrayOutputStream tempOutputStream = new ByteArrayOutputStream();
    private final ConcatenatingByteArrayCollector buf;
    private DictionaryPage dictionaryPage;

    private long uncompressedLength;
    private long compressedLength;
    private long totalValueCount;
    private int pageCount;

    private Set<Encoding> encodings = new HashSet<Encoding>();

    private Statistics totalStatistics;

    private ColumnChunkPageWriter(ColumnDescriptor path, BytesCompressor compressor, int pageSize) {
      this.path = path;
      this.compressor = compressor;

      // this writer will write many pages, so we make the initial slab size 1 page size.
      // It will then double over time until it reaches COLUMN_CHUNK_WRITER_MAX_SIZE_HINT at
      // which point it will grow linearly.
      this.buf = new ConcatenatingByteArrayCollector();
      this.totalStatistics = getStatsBasedOnType(this.path.getType());
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
      tempOutputStream.reset();
      parquetMetadataConverter.writeDataPageHeader(
          (int)uncompressedSize,
          (int)compressedSize,
          valueCount,
          statistics,
          rlEncoding,
          dlEncoding,
          valuesEncoding,
          tempOutputStream);
      buf.collect(tempOutputStream.toByteArray());
      tempOutputStream.reset();
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += valueCount;
      this.pageCount += 1;
      this.totalStatistics.mergeStatistics(statistics);
      buf.collect(compressedBytes);
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
      tempOutputStream.reset();
      parquetMetadataConverter.writeDataPageV2Header(
          uncompressedSize, compressedSize,
          valueCount, nullCount, rowCount,
          statistics,
          dataEncoding,
          rlByteLength,
          dlByteLength,
          tempOutputStream);
      buf.collect(tempOutputStream.toByteArray());
      tempOutputStream.reset();
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += valueCount;
      this.pageCount += 1;
      this.totalStatistics.mergeStatistics(statistics);
      buf.collect(repetitionLevels);
      buf.collect(definitionLevels);
      buf.collect(compressedData);
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
      writer.writeDataPages(buf, uncompressedLength, compressedLength, totalStatistics, new ArrayList<Encoding>(encodings));
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
      return buf.size();
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
  }

  private final Map<ColumnDescriptor, ColumnChunkPageWriter> writers = new HashMap<ColumnDescriptor, ColumnChunkPageWriter>();

  public ColumnChunkPageWriteStore(BytesCompressor compressor, MessageType schema, int pageSize) {
    for (ColumnDescriptor path : schema.getColumns()) {
      writers.put(path,  new ColumnChunkPageWriter(path, compressor, pageSize));
    }
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    return writers.get(path);
  }

  public void flushToFileWriter(ParquetFileWriter writer) throws IOException {
    for (ColumnChunkPageWriter pageWriter : writers.values()) {
      pageWriter.writeToFileWriter(writer);
    }
  }

}
