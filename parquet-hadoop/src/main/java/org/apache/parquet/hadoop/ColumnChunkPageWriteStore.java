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
package org.apache.parquet.hadoop;

import static org.apache.parquet.Log.INFO;
import static org.apache.parquet.column.statistics.Statistics.getStatsBasedOnType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.parquet.Log;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.ConcatenatingByteArrayCollector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.IntList;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.MessageType;

class ColumnChunkPageWriteStore implements PageWriteStore {
  private static final Log LOG = Log.getLog(ColumnChunkPageWriteStore.class);

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
    private List<PageHolder> bufferedPages = new ArrayList<PageHolder>();

    private Statistics totalStatistics;
    private ParquetProperties parquetProperties;

    private ColumnChunkPageWriter(ColumnDescriptor path,
                                  BytesCompressor compressor,
                                  ParquetProperties parquetProperties) {
      this.path = path;
      this.compressor = compressor;
      this.parquetProperties = parquetProperties;
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
      bufferedPages.add(new PageV1Holder(pageCount,
        bytes, valueCount, statistics, rlEncoding, dlEncoding, valuesEncoding));
    }

    private PageHeaderWithOffset preparePage(PageV1Holder pageV1Holder, long currentPos) throws IOException {
      final BytesInput bytes = pageV1Holder.getData();

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
      final PageHeader pageHeader = parquetMetadataConverter.writeDataPageHeader(
          (int)uncompressedSize,
          (int)compressedSize,
          pageV1Holder.getValueCount(),
          pageV1Holder.getStatistics(),
          pageV1Holder.getRlEncoding(),
          pageV1Holder.getDlEncoding(),
          pageV1Holder.getValuesEncoding(),
          tempOutputStream);
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += pageV1Holder.getValueCount();
      this.pageCount += 1;
      this.totalStatistics.mergeStatistics(pageV1Holder.getStatistics());
      // by concatenating before collecting instead of collecting twice,
      // we only allocate one buffer to copy into instead of multiple.
      buf.collect(BytesInput.concat(BytesInput.from(tempOutputStream), compressedBytes));

      encodings.add(pageV1Holder.getRlEncoding());
      encodings.add(pageV1Holder.getDlEncoding());
      encodings.add(pageV1Holder.getValuesEncoding());
      return new PageHeaderWithOffset(pageHeader, currentPos + tempOutputStream.size());
    }

    @Override
    public void writePageV2(
        int rowCount, int nullCount, int valueCount,
        BytesInput repetitionLevels, BytesInput definitionLevels,
        Encoding dataEncoding, BytesInput data,
        Statistics<?> statistics) throws IOException {
      bufferedPages.add(new PageV2Holder(pageCount,
        rowCount, nullCount, valueCount, repetitionLevels, definitionLevels, dataEncoding, data, statistics));
    }

    private PageHeaderWithOffset preparePage(PageV2Holder pageV2Holder, long currentPos) throws IOException {
      final BytesInput repetitionLevels = pageV2Holder.getRepetitionLevels();
      final BytesInput definitionLevels = pageV2Holder.getDefinitionLevels();
      final BytesInput data = pageV2Holder.getData();

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
      final PageHeader pageHeader = parquetMetadataConverter.writeDataPageV2Header(
          uncompressedSize, compressedSize,
          pageV2Holder.getValueCount(),
          pageV2Holder.getNullCount(),
          pageV2Holder.getRowCount(),
          pageV2Holder.getStatistics(),
          pageV2Holder.getValuesEncoding(),
          rlByteLength,
          dlByteLength,
          tempOutputStream);
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += pageV2Holder.getValueCount();
      this.pageCount += 1;
      this.totalStatistics.mergeStatistics(pageV2Holder.getStatistics());

      // by concatenating before collecting instead of collecting twice,
      // we only allocate one buffer to copy into instead of multiple.
      buf.collect(
          BytesInput.concat(
              BytesInput.from(tempOutputStream),
              repetitionLevels,
              definitionLevels,
              compressedData)
      );
      encodings.add(pageV2Holder.getValuesEncoding());
      return new PageHeaderWithOffset(pageHeader, currentPos + tempOutputStream.size());
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

    public List<PageHeaderWithOffset> writeBufferedPages(ParquetFileWriter writer, DictionaryPage dictionaryPage) throws IOException {
      final List<PageHeaderWithOffset> allPageHeaders = new ArrayList<PageHeaderWithOffset>();
      writer.startColumn(path, totalValueCount, compressor.getCodecName());
      if (dictionaryPage != null) {
        allPageHeaders.add(writer.writeDictionaryPage(dictionaryPage));
        encodings.add(dictionaryPage.getEncoding());
      }

      // start from current offset in output file, until now page with offsets have saved page sizes.
      long pageOffset = writer.getPos();
      for (PageHolder bufferedPage : bufferedPages) {
        final PageHeaderWithOffset pageHeader;
        if (bufferedPage instanceof PageV1Holder) {
          pageHeader = preparePage((PageV1Holder)bufferedPage, pageOffset);
        } else {
          pageHeader = preparePage((PageV2Holder)bufferedPage, pageOffset);
        }
        allPageHeaders.add(pageHeader);
        // add compressed size of this page to page offset
        pageOffset += pageHeader.getOffset() + pageHeader.getPageHeader().getCompressed_page_size();
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
      return allPageHeaders;
    }

    public List<PageHeaderWithOffset> writeToFileWriter(ParquetFileWriter writer) throws IOException {
      if (dictionaryPage == null) {
        return writeBufferedPages(writer, null);
      }
      // Copy dictionary page and create a sorted dictionary
      SortedDictionary sortedDictionary = new SortedDictionary(dictionaryPage, path, parquetProperties.getAllocator(),
        parquetProperties.getDictionaryPageSizeThreshold());

      // For each buffered page, read dictionary ids and map them to new ids.
      // Use dictionary writer to serialize newly encoded values to bytes
      for (PageHolder pageHolder : bufferedPages) {
        final BytesInput data = pageHolder.getData();
        final Encoding valuesEncoding = pageHolder.getValuesEncoding();
        final ValuesReader dictionaryBasedValuesReader =
          valuesEncoding.getDictionaryBasedValuesReader(path, ValuesType.VALUES, sortedDictionary.getDictionary());
        dictionaryBasedValuesReader.initFromPage(pageHolder.getValueCount(), data.toByteBuffer(), 0);

        final DictionaryValuesWriter valuesWriter = parquetProperties.dictionaryWriter(path, /*ignored */ (int)data.size());
        final IntList encodedValues = new IntList();
        for (int i = 0; i < pageHolder.getValueCount(); ++i) {
          final int oldDictionaryId = dictionaryBasedValuesReader.readValueDictionaryId();
          encodedValues.add(sortedDictionary.getNewId(oldDictionaryId));
        }
        BytesInput newData = valuesWriter.getBytes(encodedValues);
        pageHolder.setData(newData);
      }
      return writeBufferedPages(writer, sortedDictionary.getSortedDictionaryPage());
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
  private final MessageType schema;

  public ColumnChunkPageWriteStore(BytesCompressor compressor, MessageType schema, ParquetProperties parquetProperties) {
    this.schema = schema;
    for (ColumnDescriptor path : schema.getColumns()) {
      writers.put(path,  new ColumnChunkPageWriter(path, compressor, parquetProperties));
    }
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    return writers.get(path);
  }

  public Map<ColumnDescriptor, List<PageHeaderWithOffset>> flushToFileWriter(ParquetFileWriter writer) throws IOException {
    final Map<ColumnDescriptor, List<PageHeaderWithOffset>> pageHeaders = new HashMap<ColumnDescriptor, List<PageHeaderWithOffset>>();
    for (ColumnDescriptor path : schema.getColumns()) {
      ColumnChunkPageWriter pageWriter = writers.get(path);
      pageHeaders.put(path, pageWriter.writeToFileWriter(writer));
    }
    return pageHeaders;
  }

}
