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
package org.apache.parquet.hadoop.rewrite;

import static java.lang.String.format;
import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

class RowGroupMerger {

  private final MessageType schema;
  private final CompressionCodecFactory.BytesInputCompressor compressor;
  private final ParquetProperties parquetProperties;

  public RowGroupMerger(MessageType schema, CompressionCodecName compression, boolean useV2ValueWriter) {
    this(schema, new Configuration(), compression, useV2ValueWriter);
  }

  RowGroupMerger(MessageType schema, Configuration conf, CompressionCodecName compression, boolean useV2ValueWriter) {
    this(schema, conf, compression, createParquetProperties(useV2ValueWriter));
  }

  RowGroupMerger(MessageType schema, Configuration conf, CompressionCodecName compression, ParquetProperties parquetProperties) {
    this.schema = schema;
    this.parquetProperties = parquetProperties;
    this.compressor = new CodecFactory(conf, this.parquetProperties.getPageSizeThreshold()).getCompressor(compression);
  }

  /**
   * Merges the row groups making sure that new row groups do not exceed the supplied maxRowGroupSize
   *
   * @param inputFiles      input files to merge
   * @param maxRowGroupSize the max limit for new blocks
   * @param writer          writer to write the new blocks to
   * @throws IOException if an IO error occurs
   */
  public void merge(List<ParquetFileReader> inputFiles, final long maxRowGroupSize,
                    ParquetFileWriter writer) throws IOException {

    SizeEstimator estimator = new SizeEstimator(compressor.getCodecName() != CompressionCodecName.UNCOMPRESSED);
    MutableMergedBlock mergedBlock = null;
    for (ParquetFileReader reader : inputFiles) {
      for (BlockMetaData blockMeta : reader.getRowGroups()) {
        PageReadStore group = reader.readNextRowGroup();
        Preconditions.checkState(group != null,
          "number of groups returned by FileReader does not match metadata");

        if (mergedBlock != null && mergedBlock.getCompressedSize() + estimator.estimate(blockMeta) > maxRowGroupSize) {
          saveBlockTo(mergedBlock, writer);
          mergedBlock = null;
        }

        if (mergedBlock == null && estimator.estimate(blockMeta) > maxRowGroupSize) {
          //save it directly without re encoding it
          saveBlockTo(ReadOnlyMergedBlock.of(blockMeta, group, schema, compressor), writer);
          continue;
        }

        if (mergedBlock == null) {
          mergedBlock = new MutableMergedBlock(schema);
        }

        long sizeBeforeMerge = mergedBlock.getCompressedSize();
        mergedBlock.merge(blockMeta, group);
        //update our estimator
        long currentBlockEffect = mergedBlock.getCompressedSize() - sizeBeforeMerge;
        estimator.update(currentBlockEffect, blockMeta);
      }
    }
    if (mergedBlock != null) {
      saveBlockTo(mergedBlock, writer);
    }
    mergedBlock = null;
  }


  private void saveBlockTo(MergedBlock block, ParquetFileWriter writer) {
    try {
      writer.startBlock(block.rowCount());

      for (MergedColumn col : block.columnsInOrder()) {
        writer.startColumn(col.getColumnDesc(), col.getValueCount(), col.getCompression());

        col.writeDictionaryPageTo(writer);
        col.writeDataPagesTo(writer);

        writer.endColumn();
      }

      writer.endBlock();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static ParquetProperties createParquetProperties(boolean useV2Writer) {
    ParquetProperties.Builder builder = ParquetProperties.builder();
    if (useV2Writer) {
      builder.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0);
    }
    return builder.build();
  }

  private BytesInput compress(BytesInput bytes) {
    return compress(bytes, compressor);
  }

  private static BytesInput compress(BytesInput bytes, CompressionCodecFactory.BytesInputCompressor compressor) {
    try {
      //we copy as some compressors use shared memory
      return BytesInput.copy(compressor.compress(bytes));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private BiConsumer<ValuesReader, ValuesWriter> createWritingBridge(PrimitiveType.PrimitiveTypeName typeName) {
    switch (typeName) {
      case FIXED_LEN_BYTE_ARRAY:
      case INT96:
      case BINARY:
        return (src, dest) -> dest.writeBytes(src.readBytes());
      case BOOLEAN:
        return (src, dest) -> dest.writeBoolean(src.readBoolean());
      case DOUBLE:
        return (src, dest) -> dest.writeDouble(src.readDouble());
      case FLOAT:
        return (src, dest) -> dest.writeFloat(src.readFloat());
      case INT32:
        return (src, dest) -> dest.writeInteger(src.readInteger());
      case INT64:
        return (src, dest) -> dest.writeLong(src.readLong());
      default:
        throw new RuntimeException("Unsupported column primitive type: " + typeName.name());
    }
  }

  private static void writePageTo(DataPage dataPage, ParquetFileWriter writer) {
    dataPage.accept(new DataPage.Visitor<Void>() {
      @Override
      public Void visit(DataPageV1 page) {
        try {
          if (page.getIndexRowCount().isPresent()) {
            writer.writeDataPage(page.getValueCount(), page.getUncompressedSize(),
              page.getBytes(), page.getStatistics(), page.getIndexRowCount().get(), page.getRlEncoding(),
              page.getDlEncoding(), page.getValueEncoding());

          } else {
            writer.writeDataPage(page.getValueCount(), page.getUncompressedSize(),
              page.getBytes(), page.getStatistics(), page.getRlEncoding(),
              page.getDlEncoding(), page.getValueEncoding());
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return null;
      }

      @Override
      public Void visit(DataPageV2 page) {
        try {
          writer.writeDataPageV2(page.getRowCount(), page.getNullCount(), page.getValueCount(),
            page.getRepetitionLevels(), page.getDefinitionLevels(), page.getDataEncoding(),
            page.getData(), page.getUncompressedSize(), page.getStatistics());
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return null;
      }
    });
  }

  private static DictionaryPage getCompressedDictionary(DictionaryPage dictionary, CompressionCodecFactory.BytesInputCompressor compressor) {
    return new DictionaryPage(
      compress(dictionary.getBytes(), compressor),
      dictionary.getUncompressedSize(),
      dictionary.getDictionarySize(),
      dictionary.getEncoding());
  }

  private interface MergedBlock {
    long rowCount();

    List<MergedColumn> columnsInOrder();
  }

  private interface MergedColumn {
    long getValueCount();

    CompressionCodecName getCompression();

    ColumnDescriptor getColumnDesc();

    void writeDataPagesTo(ParquetFileWriter writer);

    void writeDictionaryPageTo(ParquetFileWriter writer) throws IOException;
  }

  private class MutableMergedBlock implements MergedBlock {

    private final Map<ColumnDescriptor, MutableMergedColumn> columns = new HashMap<>();
    private final MessageType schema;
    private long recordCount;
    private long compressedSize;

    private MutableMergedBlock(MessageType schema) {
      this.schema = schema;
    }

    @Override
    public long rowCount() {
      return recordCount;
    }

    private long getCompressedSize() {
      return compressedSize;
    }

    @Override
    public List<MergedColumn> columnsInOrder() {
      return schema.getColumns()
        .stream()
        .map(columns::get)
        .collect(Collectors.toList());
    }

    MutableMergedColumn getOrCreateColumn(ColumnDescriptor column) {
      return columns.computeIfAbsent(column, desc -> new MutableMergedColumn(desc, this::addCompressedBytes));
    }

    void addRowCount(long rowCount) {
      recordCount += rowCount;
    }

    void addCompressedBytes(long size) {
      compressedSize += size;
    }

    void merge(BlockMetaData blockMeta, PageReadStore group) throws IOException {
      for (Entry<ColumnDescriptor, ColumnChunkMetaData> col : getColumnsInOrder(blockMeta, schema)) {

        MutableMergedColumn column = getOrCreateColumn(col.getKey());
        PageReader columnReader = group.getPageReader(col.getKey());

        DictionaryPage dictPage = columnReader.readDictionaryPage();
        Dictionary decodedDictionary = null;
        if (dictPage != null) {
          decodedDictionary = dictPage.getEncoding().initDictionary(column.getColumnDesc(), dictPage);
        }

        //read all pages in this column chunk
        DataPage data;
        while ((data = columnReader.readPage()) != null) {
          column.addPage(data, decodedDictionary);
        }
      }
      addRowCount(blockMeta.getRowCount());
    }
  }

  private static List<Entry<ColumnDescriptor, ColumnChunkMetaData>> getColumnsInOrder(BlockMetaData blockMeta,
                                                                                      MessageType schema) {

    return ParquetFileWriter.getColumnsInOrder(blockMeta, schema, false).stream()
      .map(c -> toEntry(schema, c))
      .collect(Collectors.toList());
  }

  private static SimpleEntry<ColumnDescriptor, ColumnChunkMetaData> toEntry(MessageType schema,
                                                                            ColumnChunkMetaData column) {

    return new SimpleEntry<>(
      schema.getColumnDescription(column.getPath().toArray()),
      column);
  }

  private static class ReadOnlyMergedBlock implements MergedBlock {
    private final List<MergedColumn> columns;
    private final long recordCount;

    private ReadOnlyMergedBlock(List<MergedColumn> columns, long recordCount) {
      this.columns = Collections.unmodifiableList(columns);
      this.recordCount = recordCount;
    }

    @Override
    public long rowCount() {
      return recordCount;
    }

    @Override
    public List<MergedColumn> columnsInOrder() {
      return columns;
    }

    static ReadOnlyMergedBlock of(BlockMetaData blockMeta, PageReadStore group, MessageType schema,
                                  CompressionCodecFactory.BytesInputCompressor compressor) {
      List<MergedColumn> columns = new ArrayList<>();
      for (Entry<ColumnDescriptor, ColumnChunkMetaData> col : getColumnsInOrder(blockMeta, schema)) {

        List<DataPage> pages = new ArrayList<>();
        PageReader columnReader = group.getPageReader(col.getKey());

        DictionaryPage dictPage = columnReader.readDictionaryPage();
        if (dictPage != null) {
          dictPage = getCompressedDictionary(dictPage, compressor);
        }

        //read all pages in this column chunk
        DataPage data;
        while ((data = columnReader.readPage()) != null) {

          data = data.accept(new DataPage.Visitor<DataPage>() {
            @Override
            public DataPage visit(DataPageV1 pageV1) {

              return new DataPageV1(compress(pageV1.getBytes(), compressor), pageV1.getValueCount(),
                pageV1.getUncompressedSize(), pageV1.getFirstRowIndex().orElse(-1L),
                pageV1.getIndexRowCount().orElse(-1), pageV1.getStatistics(), pageV1.getRlEncoding(),
                pageV1.getDlEncoding(), pageV1.getValueEncoding());
            }

            @Override
            public DataPage visit(DataPageV2 pageV2) {

              return DataPageV2.compressed(
                pageV2.getRowCount(), pageV2.getNullCount(), pageV2.getValueCount(),
                pageV2.getRepetitionLevels(), pageV2.getDefinitionLevels(), pageV2.getDataEncoding(),
                compress(pageV2.getData(), compressor), pageV2.getUncompressedSize(), pageV2.getStatistics());
            }
          });

          pages.add(data);
        }

        ReadOnlyMergedColumn column = new ReadOnlyMergedColumn(pages, dictPage, col.getKey(),
          col.getValue().getValueCount(), compressor.getCodecName());

        columns.add(column);
      }
      return new ReadOnlyMergedBlock(columns, blockMeta.getRowCount());
    }
  }

  private static class ReadOnlyMergedColumn implements MergedColumn {
    private final List<DataPage> pages;
    private final DictionaryPage dictionary;
    private final ColumnDescriptor columnDesc;
    private final long valueCount;
    private final CompressionCodecName codecName;

    private ReadOnlyMergedColumn(List<DataPage> pages, DictionaryPage dictionary, ColumnDescriptor columnDesc,
                                 long valueCount, CompressionCodecName codecName) {
      this.pages = pages;
      this.dictionary = dictionary;
      this.columnDesc = columnDesc;
      this.valueCount = valueCount;
      this.codecName = codecName;
    }

    @Override
    public long getValueCount() {
      return valueCount;
    }

    @Override
    public CompressionCodecName getCompression() {
      return codecName;
    }

    @Override
    public ColumnDescriptor getColumnDesc() {
      return columnDesc;
    }

    @Override
    public void writeDataPagesTo(ParquetFileWriter writer) {
      pages.forEach(page -> writePageTo(page, writer));
    }

    @Override
    public void writeDictionaryPageTo(ParquetFileWriter writer) throws IOException {
      if (dictionary != null) {
        writer.writeDictionaryPage(dictionary);
      }
    }
  }

  private class MutableMergedColumn implements MergedColumn {

    private final List<DataPage> pages = new ArrayList<>();
    private final ColumnDescriptor columnDesc;
    private final ValuesWriter newValuesWriter;
    private final BiConsumer<ValuesReader, ValuesWriter> dataWriter;
    private final Consumer<Long> compressedSizeAccumulator;

    private long valueCount;

    private MutableMergedColumn(ColumnDescriptor column, Consumer<Long> compressedSizeAccumulator) {
      this.columnDesc = column;
      this.compressedSizeAccumulator = compressedSizeAccumulator;
      this.newValuesWriter = parquetProperties.newValuesWriter(columnDesc);
      this.dataWriter = createWritingBridge(columnDesc.getPrimitiveType().getPrimitiveTypeName());
    }

    @Override
    public long getValueCount() {
      return valueCount;
    }

    @Override
    public CompressionCodecName getCompression() {
      return compressor.getCodecName();
    }

    @Override
    public ColumnDescriptor getColumnDesc() {
      return columnDesc;
    }

    @Override
    public void writeDataPagesTo(ParquetFileWriter writer) {
      pages.forEach(page -> writePageTo(page, writer));
    }

    @Override
    public void writeDictionaryPageTo(ParquetFileWriter writer) throws IOException {
      DictionaryPage page = newValuesWriter.toDictPageAndClose();
      if (page != null) {
        writer.writeDictionaryPage(getCompressedDictionary(page, compressor));
        newValuesWriter.resetDictionary();
      }
    }

    void addPage(DataPage data, Dictionary pageDictionary) {
      DataPage recodePage = recodePage(data, pageDictionary);
      compressedSizeAccumulator.accept((long) recodePage.getCompressedSize());
      valueCount += recodePage.getValueCount();
      pages.add(recodePage);
    }

    DataPage recodePage(DataPage data, Dictionary pageDictionary) {
      return data.accept(new DataPage.Visitor<DataPage>() {

        @Override
        public DataPage visit(DataPageV1 pageV1) {

          try {
            verifyDataEncoding(pageV1.getValueEncoding(), pageDictionary);

            final byte[] originalBytes = pageV1.getBytes().toByteArray();

            if (pageV1.getValueEncoding().usesDictionary()) {

              ValuesReader rlReader = pageV1.getRlEncoding().getValuesReader(columnDesc, REPETITION_LEVEL);
              ValuesReader dlReader = pageV1.getDlEncoding().getValuesReader(columnDesc, DEFINITION_LEVEL);
              ValuesReader dataReader = pageV1.getValueEncoding()
                .getDictionaryBasedValuesReader(columnDesc, VALUES, pageDictionary);

              ByteBufferInputStream input = ByteBufferInputStream.wrap(ByteBuffer.wrap(originalBytes));

              int startPos = Math.toIntExact(input.position());
              rlReader.initFromPage(pageV1.getValueCount(), input);
              dlReader.initFromPage(pageV1.getValueCount(), input);
              int dlEndPos = Math.toIntExact(input.position());

              dataReader.initFromPage(pageV1.getValueCount(), input);

              int rowCount = 0;
              for (int i = 0; i < pageV1.getValueCount(); i++) {
                if (dlReader.readInteger() == columnDesc.getMaxDefinitionLevel())
                  dataWriter.accept(dataReader, newValuesWriter);

                rowCount = rlReader.readInteger() == 0 ? rowCount + 1 : rowCount;
              }

              BytesInput recodedBytes = BytesInput.concat(
                BytesInput.from(originalBytes, startPos, dlEndPos - startPos), newValuesWriter.getBytes()
              );

              Encoding valuesEncoding = newValuesWriter.getEncoding();
              int uncompressedSize = Math.toIntExact(recodedBytes.size());
              BytesInput compressedBytes = compress(recodedBytes);

              newValuesWriter.reset();

              long firstRowIndex = pageV1.getFirstRowIndex().orElse(-1L);

              return new DataPageV1(compressedBytes, pageV1.getValueCount(), uncompressedSize,
                firstRowIndex, rowCount, pageV1.getStatistics(), pageV1.getRlEncoding(),
                pageV1.getDlEncoding(), valuesEncoding
              );

            } else {
              //not dictionary encoded
              int rowCount = pageV1.getIndexRowCount()
                .orElseGet(() -> calculateRowCount(pageV1, ByteBufferInputStream.wrap(ByteBuffer.wrap(originalBytes))));

              return new DataPageV1(compress(BytesInput.from(originalBytes)), pageV1.getValueCount(),
                pageV1.getUncompressedSize(), pageV1.getFirstRowIndex().orElse(-1L),
                rowCount, pageV1.getStatistics(), pageV1.getRlEncoding(),
                pageV1.getDlEncoding(), pageV1.getValueEncoding()
              );
            }

          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }

        @Override
        public DataPage visit(DataPageV2 page) {
          verifyDataEncoding(page.getDataEncoding(), pageDictionary);

          if (page.getDataEncoding().usesDictionary()) {

            ValuesReader reader = page.getDataEncoding()
              .getDictionaryBasedValuesReader(columnDesc, VALUES, pageDictionary);

            try {
              reader.initFromPage(page.getValueCount(), page.getData().toInputStream());
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }

            for (int i = 0; i < page.getValueCount() - page.getNullCount(); i++) {
              dataWriter.accept(reader, newValuesWriter);
            }

            BytesInput dataBytes = newValuesWriter.getBytes();
            Encoding dataEncoding = newValuesWriter.getEncoding();
            int uncompressedSize = Math.toIntExact(dataBytes.size());
            BytesInput compressedBytes = compress(dataBytes);

            newValuesWriter.reset();

            return DataPageV2.compressed(
              page.getRowCount(), page.getNullCount(), page.getValueCount(),
              page.getRepetitionLevels(), page.getDefinitionLevels(), dataEncoding,
              compressedBytes, uncompressedSize, page.getStatistics()
            );

          } else { //not dictionary encoded

            return DataPageV2.compressed(
              page.getRowCount(), page.getNullCount(), page.getValueCount(),
              page.getRepetitionLevels(), page.getDefinitionLevels(), page.getDataEncoding(),
              compress(page.getData()), page.getUncompressedSize(), page.getStatistics());
          }
        }
      });
    }

    private void verifyDataEncoding(Encoding encoding, Dictionary pageDictionary) {
      if (encoding.usesDictionary() && pageDictionary == null) {
        throw new ParquetDecodingException(
          format("could not read page in column %s as the dictionary was missing for encoding %s", columnDesc, encoding));
      }
    }

    //expensive should be used as a last resort
    private int calculateRowCount(DataPageV1 pageV1, ByteBufferInputStream in) {
      try {
        ValuesReader rlReader = pageV1.getRlEncoding().getValuesReader(columnDesc, REPETITION_LEVEL);
        rlReader.initFromPage(pageV1.getValueCount(), in);
        int rowCount = 0;
        for (int i = 0; i < pageV1.getValueCount(); i++) {
          rowCount = rlReader.readInteger() == 0 ? rowCount + 1 : rowCount;
        }
        return rowCount;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static class SizeEstimator {
    //is destination block compressed
    private final boolean targetCompressed;
    private double factor;

    private SizeEstimator(boolean targetCompressed) {
      this.targetCompressed = targetCompressed;
    }

    private long estimate(BlockMetaData blockMeta) {
      if (factor <= 0) {
        //very naive estimator
        return targetCompressed ? blockMeta.getCompressedSize() : blockMeta.getTotalByteSize();
      }
      return (long) (factor * blockMeta.getTotalByteSize());
    }

    private void update(long actualBytes, BlockMetaData blockMeta) {
      // basically remembers last ratio
      factor = (double) actualBytes / blockMeta.getTotalByteSize();
    }
  }
}
