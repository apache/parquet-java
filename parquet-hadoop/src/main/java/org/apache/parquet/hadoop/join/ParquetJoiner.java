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
package org.apache.parquet.hadoop.join;

import org.apache.curator.shaded.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.*;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.parquet.column.ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;

public class ParquetJoiner implements Closeable {

  // Key to store original writer version in the file key-value metadata
  public static final String ORIGINAL_CREATED_BY_KEY = "original.created.by";
  private static final Logger LOG = LoggerFactory.getLogger(ParquetJoiner.class);

  // Configurations for the new file
  private final Map<String, String> extraMetaData;
  // Writer to rewrite the input files
  private final ParquetFileWriter writer;

  // Reader and relevant states of the in-processing input file
  private final Queue<TransParquetFileReader> inputFilesL;
  private final List<RightColumnWriter> columnWritersR = new ArrayList<>();
  private final Map<ColumnPath, ColumnDescriptor> descriptorsMapL;
  // The index cache strategy
  private final IndexCache.CacheStrategy indexCacheStrategy;

  public ParquetJoiner(JoinOptions options) throws IOException {
    ParquetConfiguration conf = options.getParquetConfiguration();
    OutputFile outFile = options.getParquetOutputFile();
    this.inputFilesL = getFileReaders(options.getParquetInputFilesL(), conf);
    List<Queue<TransParquetFileReader>> inputFilesR = options.getParquetInputFilesR()
        .stream()
        .map(x -> getFileReaders(x, conf))
        .collect(Collectors.toList());
    ensureSameSchema(inputFilesL);
    inputFilesR.forEach(this::ensureSameSchema);
    LOG.info("Start rewriting {} input file(s) {} to {}", inputFilesL.size(), options.getParquetInputFilesL(), outFile); // TODO add logging for all the files

    this.extraMetaData = ImmutableMap.of(
        ORIGINAL_CREATED_BY_KEY,
        Stream.concat(inputFilesL.stream(), inputFilesR.stream().flatMap(Collection::stream))
            .map(x -> x.getFooter().getFileMetaData().getCreatedBy())
            .reduce((a, b) -> a + "\n" + b)
            .orElse("")
    );

    // TODO check that schema on the left and on the right is not identical
    MessageType schemaL = inputFilesL.peek().getFooter().getFileMetaData().getSchema();
    List<MessageType> schemaR = inputFilesR
        .stream()
        .map(x -> x.peek().getFooter().getFileMetaData().getSchema())
        .collect(Collectors.toList());

    // TODO check that there is no overlap of fields on the right
    Map<String, Type> fieldNamesL = schemaL.getFields().stream().collect(Collectors.toMap(Type::getName, x -> x));
    Map<String, Type> fieldNamesR = schemaR.stream().flatMap(x -> x.getFields().stream()).collect(Collectors.toMap(Type::getName, x -> x));
    List<Type> fields = Stream.concat(
        fieldNamesL.values().stream().map(x -> fieldNamesR.getOrDefault(x.getName(), x)), // take a field on the right if we can
        fieldNamesR.values().stream().filter(x -> !fieldNamesL.containsKey(x.getName())) // takes fields on the right if it was not present on the left
    ).collect(Collectors.toList());
    // Schema of input files (should be the same) and to write to the output file
    MessageType schema = new MessageType(schemaL.getName(), fields);

    this.descriptorsMapL = schemaL.getColumns().stream()
        .filter(x -> x.getPath().length == 0 || !fieldNamesR.containsKey(x.getPath()[0]))
        .collect(Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
    this.indexCacheStrategy = options.getIndexCacheStrategy();

    long rowCountL = inputFilesL.stream().mapToLong(ParquetFileReader::getRecordCount).sum();
    long rowCountR = inputFilesR.stream().flatMap(Collection::stream).mapToLong(ParquetFileReader::getRecordCount).sum();
    if (rowCountL != rowCountR) {
      throw new IllegalArgumentException("The number of records on the left and on the right don't match!");
    }

    ParquetFileWriter.Mode writerMode = ParquetFileWriter.Mode.CREATE;
    this.writer = new ParquetFileWriter(
        outFile,
        schema,
        writerMode,
        DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT,
        DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
        DEFAULT_STATISTICS_TRUNCATE_LENGTH,
        ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED);
    this.writer.start();

    for (Queue<TransParquetFileReader> inFiles: inputFilesR) {
      this.columnWritersR.add(new RightColumnWriter(inFiles, writer));
    }
  }

  // Open all input files to validate their schemas are compatible to merge
  private Queue<TransParquetFileReader> getFileReaders(List<InputFile> inputFiles, ParquetConfiguration conf) {
    Preconditions.checkArgument(inputFiles != null && !inputFiles.isEmpty(), "No input files");
    LinkedList<TransParquetFileReader> inputFileReaders = new LinkedList<>();
    for (InputFile inputFile : inputFiles) {
      try {
        TransParquetFileReader reader = new TransParquetFileReader(
            inputFile, ParquetReadOptions.builder(conf).build());
        inputFileReaders.add(reader);
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to open input file: " + inputFile, e);
      }
    }
    return inputFileReaders;
  }

  private void ensureSameSchema(Queue<TransParquetFileReader> inputFileReaders) {
    MessageType schema = null;
    for (TransParquetFileReader reader : inputFileReaders) {
      MessageType newSchema = reader.getFooter().getFileMetaData().getSchema();
      if (schema == null) {
        schema = newSchema;
      } else {
        // Now we enforce equality of schemas from input files for simplicity.
        if (!schema.equals(newSchema)) {
          String file = reader.getFile();
          LOG.error(
              "Input files have different schemas, expect: {}, input: {}, current file: {}",
              schema,
              newSchema,
              file);
          throw new InvalidSchemaException(
              "Input files have different schemas, current file: " + file);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    writer.end(extraMetaData);
  }

  // TODO add the test for empty files joins, it should merge schemas
  public void processBlocks() throws IOException {
    int rowGroupIdx = 0;
    while (!inputFilesL.isEmpty()) {
      TransParquetFileReader reader = inputFilesL.poll();
      IndexCache indexCache = IndexCache.create(reader, descriptorsMapL.keySet(), indexCacheStrategy, true);
      LOG.info("Rewriting input fileLeft: {}, remaining filesLeft: {}", reader.getFile(), inputFilesL.size());
      List<BlockMetaData> blocks = reader.getFooter().getBlocks();
      for (BlockMetaData blockMetaData: blocks) {
        writer.startBlock(blockMetaData.getRowCount());

        // Writing the left side
        indexCache.setBlockMetadata(blockMetaData);
        List<ColumnChunkMetaData> chunksL = blockMetaData.getColumns();
        for (ColumnChunkMetaData chunk : chunksL) {
          if (chunk.isEncrypted()) { // TODO add that detail to docs
            throw new IOException("Column " + chunk.getPath().toDotString() + " is encrypted");
          }
          ColumnDescriptor descriptorL = descriptorsMapL.get(chunk.getPath());
          if (descriptorL != null) { // descriptorL might be NULL if a column is from the right side of a join
            reader.setStreamPosition(chunk.getStartingPos());
            BloomFilter bloomFilter = indexCache.getBloomFilter(chunk);
            ColumnIndex columnIndex = indexCache.getColumnIndex(chunk);
            OffsetIndex offsetIndex = indexCache.getOffsetIndex(chunk);
            writer.appendColumnChunk(descriptorL, reader.getStream(), chunk, bloomFilter, columnIndex, offsetIndex);
          }
        }

        // Writing the right side
        for (RightColumnWriter writer: columnWritersR) {
          writer.writeRows(rowGroupIdx, blockMetaData.getRowCount());
        }

        writer.endBlock();
        rowGroupIdx++;
      }
    }
  }

  private static class RightColumnWriter {
    private final Queue<TransParquetFileReader> inputFiles;
    private final ParquetFileWriter writer;
    private final MessageType schema;
    private final Map<ColumnPath, ColumnDescriptor> descriptorsMapR;
    private final Map<ColumnDescriptor, ColumnReader> colReadersR = new HashMap<>();
    private int rowGroupIdxR = 0;
    private int writtenFromBlock = 0;

    public RightColumnWriter(Queue<TransParquetFileReader> inputFiles, ParquetFileWriter writer) throws IOException {
      this.inputFiles = inputFiles;
      this.writer = writer;
      this.schema = inputFiles.peek().getFooter().getFileMetaData().getSchema();
      this.descriptorsMapR =
          this.schema.getColumns().stream().collect(Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
      initColumnReaders();
    }

    public void writeRows(int rowGroupIdx, long rowsToWrite) throws IOException {
//       LOG.info("Rewriting input fileRight: {}, remaining fileRight: {}", readerR.getFile(), inputFilesR.size());
      while (rowsToWrite > 0) {
        List<BlockMetaData> blocks = inputFiles.peek().getFooter().getBlocks();
        BlockMetaData block = blocks.get(rowGroupIdxR);
        List<ColumnChunkMetaData> chunksR = block.getColumns();
        long leftInBlock = block.getRowCount() - writtenFromBlock;
        long writeFromBlock = Math.min(rowsToWrite, leftInBlock);
        for (ColumnChunkMetaData chunkR : chunksR) {
          if (chunkR.isEncrypted()) {
            throw new IOException("Column " + chunkR.getPath().toDotString() + " is encrypted"); // TODO add that detail to docs
          }
          ColumnDescriptor descriptorR = descriptorsMapR.get(chunkR.getPath());
          ColumnReader colReaderR = colReadersR.get(descriptorR);
          MessageType columnSchema = newSchema(schema, descriptorR);
          writeRows(colReaderR, writer, descriptorR, chunkR, columnSchema, writeFromBlock, rowGroupIdx);
        }
        rowsToWrite -= writeFromBlock;
        writtenFromBlock += writeFromBlock;
        if (rowsToWrite > 0) {
          rowGroupIdxR++;
        }
        if (rowGroupIdxR == blocks.size()) {
          inputFiles.poll();
          rowGroupIdxR = 0;
          writtenFromBlock = 0;
        }
        initColumnReaders();
      }
    }

    private void initColumnReaders() throws IOException {
      if (!inputFiles.isEmpty()) {
        for (ColumnDescriptor descriptorR : descriptorsMapR.values()) {
          TransParquetFileReader readerR = inputFiles.peek();
          PageReadStore pageReadStore = readerR.readRowGroup(rowGroupIdxR);
          String createdBy = readerR.getFooter().getFileMetaData().getCreatedBy();
          ColumnReadStoreImpl crStore =
              new ColumnReadStoreImpl(pageReadStore, new DummyGroupConverter(), schema, createdBy);
          ColumnReader cReader = crStore.getColumnReader(descriptorR);
          colReadersR.put(descriptorR, cReader);
        }
      }
    }

    private void writeRows(
        ColumnReader reader,
        ParquetFileWriter writer,
        ColumnDescriptor descriptor,
        ColumnChunkMetaData chunk,
        MessageType columnSchema,
        long rowsToWrite,
        int rowGroupIdx)
        throws IOException {

      int dMax = descriptor.getMaxDefinitionLevel();
      ParquetProperties.WriterVersion writerVersion = chunk.getEncodingStats().usesV2Pages()
          ? ParquetProperties.WriterVersion.PARQUET_2_0
          : ParquetProperties.WriterVersion.PARQUET_1_0;
      ParquetProperties props =
          ParquetProperties.builder().withWriterVersion(writerVersion).build();
      CodecFactory codecFactory = new CodecFactory(new Configuration(), props.getPageSizeThreshold());
      CompressionCodecFactory.BytesInputCompressor compressor = codecFactory.getCompressor(chunk.getCodec());

      // Create new schema that only has the current column
      ColumnChunkPageWriteStore cPageStore = new ColumnChunkPageWriteStore(
          compressor,
          columnSchema,
          props.getAllocator(),
          props.getColumnIndexTruncateLength(),
          props.getPageWriteChecksumEnabled(),
          writer.getEncryptor(),
          rowGroupIdx);
      ColumnWriteStore cStore = props.newColumnWriteStore(columnSchema, cPageStore);
      ColumnWriter cWriter = cStore.getColumnWriter(descriptor);
      Class<?> columnType = descriptor.getPrimitiveType().getPrimitiveTypeName().javaType;

      int rowCount = 0;
      while (rowCount < rowsToWrite) {
        int rlvl = reader.getCurrentRepetitionLevel();
        int dlvl = reader.getCurrentDefinitionLevel();
        if (rlvl == 0) {
          if (rowCount > 0) {
            cStore.endRecord();
          }
          rowCount++;
        }
        if (dlvl < dMax) {
          cWriter.writeNull(rlvl, dlvl);
        } else if (columnType == Integer.TYPE) {
          cWriter.write(reader.getInteger(), rlvl, dlvl);
        } else if (columnType == Long.TYPE) {
          cWriter.write(reader.getLong(), rlvl, dlvl);
        } else if (columnType == Float.TYPE) {
          cWriter.write(reader.getFloat(), rlvl, dlvl);
        } else if (columnType == Double.TYPE) {
          cWriter.write(reader.getDouble(), rlvl, dlvl);
        } else if (columnType == Binary.class) {
          cWriter.write(reader.getBinary(), rlvl, dlvl);
        } else if (columnType == Boolean.TYPE) {
          cWriter.write(reader.getBoolean(), rlvl, dlvl);
        } else {
          throw new UnsupportedOperationException(
              String.format("Unsupported column java class: %s", columnType.toString()));
        }
        reader.consume();
      }
      cStore.endRecord();

      cStore.flush();
      cPageStore.flushToFileWriter(writer);

      cStore.close();
      cWriter.close();
    }

    private MessageType newSchema(MessageType schema, ColumnDescriptor descriptor) {
      String[] path = descriptor.getPath();
      Type type = schema.getType(path);
      if (path.length == 1) {
        return new MessageType(schema.getName(), type);
      }

      for (Type field : schema.getFields()) {
        if (!field.isPrimitive()) {
          Type newType = extractField(field.asGroupType(), type);
          if (newType != null) {
            return new MessageType(schema.getName(), newType);
          }
        }
      }

      // We should never hit this because 'type' is returned by schema.getType().
      throw new RuntimeException("No field is found");
    }

    private Type extractField(GroupType candidate, Type targetField) {
      if (targetField.equals(candidate)) {
        return targetField;
      }

      // In case 'type' is a descendants of candidate
      for (Type field : candidate.asGroupType().getFields()) {
        if (field.isPrimitive()) {
          if (field.equals(targetField)) {
            return new GroupType(candidate.getRepetition(), candidate.getName(), targetField);
          }
        } else {
          Type tempField = extractField(field.asGroupType(), targetField);
          if (tempField != null) {
            return new GroupType(candidate.getRepetition(), candidate.getName(), tempField);
          }
        }
      }

      return null;
    }

    private static final class DummyGroupConverter extends GroupConverter {
      @Override
      public void start() {}

      @Override
      public void end() {}

      @Override
      public Converter getConverter(int fieldIndex) {
        return new DummyConverter();
      }
    }

    private static final class DummyConverter extends PrimitiveConverter {
      @Override
      public GroupConverter asGroupConverter() {
        return new DummyGroupConverter();
      }
    }

  }

}
