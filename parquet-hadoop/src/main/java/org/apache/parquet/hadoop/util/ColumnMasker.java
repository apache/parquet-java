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

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileWriter;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ColumnMasker {
  /**
   *
   * @param reader Reader of source file
   * @param writer Writer of destination file
   * @param meta Metadata of source file
   * @param schema Schema of source file
   * @param paths Column Paths need to be masked
   * @param maskMode Mode to mask
   * @throws IOException
   */
  public void processBlocks(TransParquetFileReader reader, TransParquetFileWriter writer, ParquetMetadata meta,
                            MessageType schema, List<String> paths, MaskMode maskMode) throws IOException {
    Set<ColumnPath> nullifyColumns = convertToColumnPaths(paths);
    int blockIndex = 0;
    PageReadStore store = reader.readNextRowGroup();

    while (store != null) {
      writer.startBlock(store.getRowCount());
      List<ColumnChunkMetaData> columnsInOrder = meta.getBlocks().get(blockIndex).getColumns();
      Map<ColumnPath, ColumnDescriptor> descriptorsMap = schema.getColumns().stream().collect(
        Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
      ColumnReadStoreImpl crStore = new ColumnReadStoreImpl(store, new DummyGroupConverter(), schema,
        meta.getFileMetaData().getCreatedBy());

      for (int i = 0; i < columnsInOrder.size(); i += 1) {
        ColumnChunkMetaData chunk = columnsInOrder.get(i);
        ColumnDescriptor descriptor = descriptorsMap.get(chunk.getPath());
        processChunk(descriptor, chunk, crStore, reader, writer, schema, nullifyColumns, maskMode);
      }

      writer.endBlock();
      store = reader.readNextRowGroup();
      blockIndex++;
    }
  }

  private void processChunk(ColumnDescriptor descriptor, ColumnChunkMetaData chunk, ColumnReadStoreImpl crStore,
                            TransParquetFileReader reader, TransParquetFileWriter writer, MessageType schema,
                            Set<ColumnPath> paths, MaskMode maskMode) throws IOException {
    reader.setStreamPosition(chunk.getStartingPos());

    if (paths.contains(chunk.getPath())) {
      if (maskMode.equals(MaskMode.NULLIFY)) {
        Type.Repetition repetition = descriptor.getPrimitiveType().getRepetition();
        if (repetition.equals(Type.Repetition.REQUIRED)) {
          throw new IOException("Required column [" + descriptor.getPrimitiveType().getName() + "] cannot be nullified");
        }
        nullifyColumn(descriptor, chunk, crStore, writer, schema);
      } else {
        throw new UnsupportedOperationException("Only nullify is supported for now");
      }
    } else {
      BloomFilter bloomFilter = reader.readBloomFilter(chunk);
      ColumnIndex columnIndex = reader.readColumnIndex(chunk);
      OffsetIndex offsetIndex = reader.readOffsetIndex(chunk);
      writer.appendColumnChunk(descriptor, reader.getStream(), chunk, bloomFilter, columnIndex, offsetIndex);
    }
  }

  private void nullifyColumn(ColumnDescriptor descriptor, ColumnChunkMetaData chunk, ColumnReadStoreImpl crStore,
                             TransParquetFileWriter writer, MessageType schema) throws IOException {
    long totalChunkValues = chunk.getValueCount();
    int dMax = descriptor.getMaxDefinitionLevel();
    ColumnReader cReader = crStore.getColumnReader(descriptor);

    writer.startColumn(descriptor, totalChunkValues, CompressionCodecName.UNCOMPRESSED);

    WriterVersion writerVersion = chunk.getEncodingStats().usesV2Pages() ? WriterVersion.PARQUET_2_0 : WriterVersion.PARQUET_1_0;
    ParquetProperties props = ParquetProperties.builder()
      .withWriterVersion(writerVersion)
      .build();
    ColumnWriter cWriter = props.newColumnWriteStore(schema, new DummyPageWriterStore()).getColumnWriter(descriptor);

    for (int i = 0; i < totalChunkValues; i++) {
      int rlvl = cReader.getCurrentRepetitionLevel();
      int dlvl = cReader.getCurrentDefinitionLevel();
      if (dlvl == dMax) {
        // since we checked ether optional or repeated, dlvl should be > 0
        if (dlvl == 0) {
          throw new IOException("definition level is detected to be 0 for column " + chunk.getPath().toDotString() + " to be nullified");
        }
        // we just write one null for the whole list at the top level, instead of nullify the elements in the list one by one
        if (rlvl == 0) {
          cWriter.writeNull(rlvl, dlvl - 1);
        }
      } else {
        cWriter.writeNull(rlvl, dlvl);
      }
    }

    BytesInput data = cWriter.concatWriters();
    Statistics statistics = convertStatisticsNullify(chunk.getPrimitiveType(), totalChunkValues);
    writer.writeDataPage(toIntWithCheck(totalChunkValues),
      toIntWithCheck(data.size()),
      data,
      statistics,
      toIntWithCheck(totalChunkValues),
      Encoding.RLE,
      Encoding.RLE,
      Encoding.PLAIN);
    writer.endColumn();
  }

  private Statistics convertStatisticsNullify(PrimitiveType type, long rowCount) throws IOException {
    org.apache.parquet.column.statistics.Statistics.Builder statsBuilder = org.apache.parquet.column.statistics.Statistics.getBuilderForReading(type);
    statsBuilder.withNumNulls(rowCount);
    return statsBuilder.build();
  }

  private int toIntWithCheck(long size) {
    if ((int)size != size) {
      throw new ParquetEncodingException("size is bigger than " + Integer.MAX_VALUE + " bytes: " + size);
    }
    return (int)size;
  }

  public static Set<ColumnPath> convertToColumnPaths(List<String> cols) {
    Set<ColumnPath> prunePaths = new HashSet<>();
    for (String col : cols) {
      prunePaths.add(ColumnPath.fromDotString(col));
    }
    return prunePaths;
  }

  public enum MaskMode {
    NULLIFY("nullify"),
    HASH("hash"),
    REDACT("redact");

    private String mode;

    MaskMode(String text) {
      this.mode = text;
    }

    public String getMode() {
      return this.mode;
    }

    public static MaskMode fromString(String mode) {
      for (MaskMode b : MaskMode.values()) {
        if (b.mode.equalsIgnoreCase(mode)) {
          return b;
        }
      }
      return null;
    }
  }

  private static final class DummyGroupConverter extends GroupConverter {
    @Override public void start() {}
    @Override public void end() {}
    @Override public Converter getConverter(int fieldIndex) { return new DummyConverter(); }
  }

  private static final class DummyConverter extends PrimitiveConverter {
    @Override public GroupConverter asGroupConverter() { return new DummyGroupConverter(); }
  }

  class DummyPageWriterStore implements PageWriteStore {
    @Override
    public PageWriter getPageWriter(ColumnDescriptor path){
      return new DummyPageWriter();
    }
  }

  class DummyPageWriter implements PageWriter {

    public DummyPageWriter() {}

    @Override
    public void writePage(BytesInput bytesInput, int valueCount, Statistics statistics, Encoding rlEncoding,
                          Encoding dlEncoding, Encoding valuesEncoding)
      throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writePage(BytesInput bytesInput, int valueCount, int rowCount, Statistics<?> statistics,
                          Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) throws IOException {
      writePage(bytesInput, valueCount, statistics, rlEncoding, dlEncoding, valuesEncoding);
    }

    @Override
    public void writePageV2(int rowCount, int nullCount, int valueCount,
                            BytesInput repetitionLevels, BytesInput definitionLevels,
                            Encoding dataEncoding, BytesInput data, Statistics<?> statistics) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getMemSize() {
      throw new UnsupportedOperationException();
    }

    public List<DataPage> getPages() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long allocatedSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String memUsageString(String prefix) {
      throw new UnsupportedOperationException();
    }
  }
}
