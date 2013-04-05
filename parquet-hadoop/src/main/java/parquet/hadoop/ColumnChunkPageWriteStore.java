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

import static parquet.Log.DEBUG;
import static parquet.Log.INFO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageWriteStore;
import parquet.column.page.PageWriter;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactory.BytesCompressor;
import parquet.schema.MessageType;

class ColumnChunkPageWriteStore implements PageWriteStore {
  private static final Log LOG = Log.getLog(ColumnChunkPageWriteStore.class);

  private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

  private static final class ColumnChunkPageWriter implements PageWriter {

    private final ColumnDescriptor path;
    private final BytesCompressor compressor;

    private final CapacityByteArrayOutputStream buf;
    private final List<DictionaryPage> dictionaryPages;

    private long uncompressedLength;
    private long compressedLength;
    private long totalValueCount;
    private int pageCount;

    private Set<Encoding> encodings = new HashSet<Encoding>();

    private ColumnChunkPageWriter(ColumnDescriptor path, BytesCompressor compressor, int initialSize) {
      this.path = path;
      this.compressor = compressor;
      this.buf = new CapacityByteArrayOutputStream(initialSize);
      this.dictionaryPages = new ArrayList<DictionaryPage>();
    }

    @Override
    public void writePage(BytesInput bytes, int valueCount, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) throws IOException {
      long uncompressedSize = bytes.size();
      BytesInput compressedBytes = compressor.compress(bytes);
      long compressedSize = compressedBytes.size();
      parquetMetadataConverter.writeDataPageHeader(
          (int)uncompressedSize,
          (int)compressedSize,
          valueCount,
          rlEncoding,
          dlEncoding,
          valuesEncoding,
          buf);
      this.uncompressedLength += uncompressedSize;
      this.compressedLength += compressedSize;
      this.totalValueCount += valueCount;
      this.pageCount += 1;
      compressedBytes.writeAllTo(buf);
      encodings.add(rlEncoding);
      encodings.add(dlEncoding);
      encodings.add(valuesEncoding);
    }

    @Override
    public long getMemSize() {
      return buf.size();
    }

    public void writeToFileWriter(ParquetFileWriter writer) throws IOException {
      writer.startColumn(path, totalValueCount, compressor.getCodecName());
      int dicEntriesTotal = 0;
      int dicByteSizeTotal = 0;
      int dicByteSizeUncTotal = 0;
      for (DictionaryPage dictionaryPage : dictionaryPages) {
        writer.writeDictionaryPage(dictionaryPage);
        encodings.add(dictionaryPage.getEncoding());
        dicEntriesTotal += dictionaryPage.getDictionarySize();
        dicByteSizeTotal += dictionaryPage.getBytes().size();
        dicByteSizeUncTotal += dictionaryPage.getUncompressedSize();
      }
      writer.writeDataPages(BytesInput.from(buf), uncompressedLength, compressedLength, new ArrayList<Encoding>(encodings));
      writer.endColumn();
      if (INFO) {
        LOG.info("written Column chunk " + path + ": "
            + totalValueCount + " values, "
            + uncompressedLength + "B raw, "
            + compressedLength + "B comp, "
            + pageCount + " pages, "
            + "encodings: " + encodings + ", "
            + (dictionaryPages.size() > 0 ? "dic { "
            + dictionaryPages.size() + " pages, "
            + dicEntriesTotal + " entries, "
            + dicByteSizeUncTotal + "B raw, " +
            + dicByteSizeTotal + "B comp}, " : "")
            + "totalSize: " + buf.size() + "B");
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
      BytesInput dictionaryBytes = dictionaryPage.getBytes();
      int uncompressedSize = (int)dictionaryBytes.size();
      BytesInput compressedBytes = compressor.compress(dictionaryBytes);
      dictionaryPages.add(new DictionaryPage(BytesInput.copy(compressedBytes), uncompressedSize, dictionaryPage.getDictionarySize(), dictionaryPage.getEncoding()));
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkPageWriter> writers = new HashMap<ColumnDescriptor, ColumnChunkPageWriter>();
  private final MessageType schema;
  private final BytesCompressor compressor;

  public ColumnChunkPageWriteStore(BytesCompressor compressor, MessageType schema) {
    this.compressor = compressor;
    this.schema = schema;
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    if (!writers.containsKey(path)) {
      writers.put(path,  new ColumnChunkPageWriter(path, compressor, 1024*1024/2)); // TODO: better deal with this initial size
    }
    return writers.get(path);
  }

  public void flushToFileWriter(ParquetFileWriter writer) throws IOException {
    List<ColumnDescriptor> columns = schema.getColumns();
    for (ColumnDescriptor columnDescriptor : columns) {
      ColumnChunkPageWriter pageWriter = writers.get(columnDescriptor);
      pageWriter.writeToFileWriter(writer);
    }
  }

}
