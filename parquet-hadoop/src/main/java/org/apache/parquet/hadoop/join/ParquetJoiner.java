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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesInput;
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
  private final int pageBufferSize = ParquetProperties.DEFAULT_PAGE_SIZE * 2;
  private final byte[] pageBuffer = new byte[pageBufferSize];

  // Configurations for the new file
  private final Map<String, String> extraMetaData;
  // Writer to rewrite the input files
  private final ParquetFileWriter writer;

  // Reader and relevant states of the in-processing input file
  private final Queue<TransParquetFileReader> inputFilesL;
  private final Queue<TransParquetFileReader> inputFilesR;
  // Schema of input files (should be the same) and to write to the output file
  private final MessageType schema;
  private final MessageType schemaL;
  private final MessageType schemaR;
  private final Map<ColumnPath, ColumnDescriptor> descriptorsMapL;
  private final Map<ColumnPath, ColumnDescriptor> descriptorsMapR;
  // created_by information of current reader being processed
  private String originalCreatedBy = "";
  // Unique created_by information from all input files
//   private final Set<String> allOriginalCreatedBys = new HashSet<>();
  // The index cache strategy
  private final IndexCache.CacheStrategy indexCacheStrategy;

  public ParquetJoiner(JoinOptions options) throws IOException {
    ParquetConfiguration conf = options.getParquetConfiguration();
    OutputFile outFile = options.getParquetOutputFile();
    this.inputFilesL = getFileReader(options.getParquetInputFilesL(), conf);
    this.inputFilesR = getFileReader(options.getParquetInputFilesR(), conf);

    Map<String, String> map = new HashMap<>();
    map.put(
      ORIGINAL_CREATED_BY_KEY,
      String.join(
        "\n",
        Stream.concat(inputFilesL.stream(), inputFilesR.stream())
          .map(x -> x.getFooter().getFileMetaData().getCreatedBy())
          .collect(Collectors.toSet())
      )
    );
    this.extraMetaData = Collections.unmodifiableMap(map);

    LOG.info("Start rewriting {} input file(s) {} to {}", inputFilesL.size(), options.getParquetInputFilesL(), outFile); // TODO

    this.schemaL = getInputFilesSchema(inputFilesL);
    this.schemaR = getInputFilesSchema(inputFilesR);

    Set<String> fieldNamesR = schemaR.getFields().stream().map(Type::getName).collect(Collectors.toSet());
    List<Type> fields = Stream.concat(
        schemaL.getFields().stream().filter(x -> !fieldNamesR.contains(x.getName())),
        schemaR.getFields().stream()
    ).collect(Collectors.toList());
    this.schema = new MessageType(schemaL.getName(), fields);

    this.descriptorsMapL = schemaL.getColumns().stream()
        .filter(x -> x.getPath().length == 0 || !fieldNamesR.contains(x.getPath()[0]))
        .collect(Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
    this.descriptorsMapR = schemaR.getColumns().stream()
        .collect(Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
    this.indexCacheStrategy = options.getIndexCacheStrategy();

    long rowCountL = inputFilesL.stream().mapToLong(ParquetFileReader::getRecordCount).sum();
    long rowCountR = inputFilesR.stream().mapToLong(ParquetFileReader::getRecordCount).sum();
    if (rowCountL != rowCountR) {
      throw new IllegalArgumentException("The number of records on the left and on the right don't match!");
    }

    ParquetFileWriter.Mode writerMode = ParquetFileWriter.Mode.CREATE;
    writer = new ParquetFileWriter(
        outFile,
        schema,
        writerMode,
        DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT,
        DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
        DEFAULT_STATISTICS_TRUNCATE_LENGTH,
        ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED);
    writer.start();
  }

  // Open all input files to validate their schemas are compatible to merge
  private Queue<TransParquetFileReader> getFileReader(List<InputFile> inputFiles, ParquetConfiguration conf) {
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

  private MessageType getInputFilesSchema(Queue<TransParquetFileReader> inputFileReaders) {
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
    return schema;
  }

  @Override
  public void close() throws IOException {
    writer.end(extraMetaData);
  }

//   private static class ColumnReaderIterator implements Iterator<ColumnReader> {
//
//     @Override
//     public boolean hasNext() {
//       return false;
//     }
//
//     @Override
//     public ColumnReader next() {
//       return null;
//     }
//   }

  public void processBlocks() throws IOException {
    int numBlocksRewritten = 0;
//     new ColumnReaderIterator();
    Map<ColumnDescriptor, ColumnReader> colReadersR = new HashMap<>();
    int blockIdR = 0;
    int writtenInBlock = 0;
    while (!inputFilesL.isEmpty()) {


      TransParquetFileReader readerL = inputFilesL.poll();
      IndexCache indexCacheL = IndexCache.create(readerL, descriptorsMapL.keySet(), indexCacheStrategy, true);
      LOG.info("Rewriting input fileLeft: {}, remaining filesLeft: {}", readerL.getFile(), inputFilesL.size());
      ParquetMetadata metaL = readerL.getFooter();
      for (int blockId = 0; blockId < metaL.getBlocks().size(); blockId++) {
        BlockMetaData blockMetaDataL = metaL.getBlocks().get(blockId);
        writer.startBlock(blockMetaDataL.getRowCount());


        indexCacheL.setBlockMetadata(blockMetaDataL);
        List<ColumnChunkMetaData> chunksL = blockMetaDataL.getColumns();
        for (ColumnChunkMetaData chunk : chunksL) {
          // TODO add that detail to docs
          if (chunk.isEncrypted()) { // If a column is encrypted we simply throw exception.
            throw new IOException("Column " + chunk.getPath().toDotString() + " is encrypted");
          }
          ColumnDescriptor descriptorL = descriptorsMapL.get(chunk.getPath());
          if (descriptorL != null) { // descriptorL might be NULL if a column is from the right side of a join
            readerL.setStreamPosition(chunk.getStartingPos());
            BloomFilter bloomFilter = indexCacheL.getBloomFilter(chunk);
            ColumnIndex columnIndex = indexCacheL.getColumnIndex(chunk);
            OffsetIndex offsetIndex = indexCacheL.getOffsetIndex(chunk);
            writer.appendColumnChunk(descriptorL, readerL.getStream(), chunk, bloomFilter, columnIndex, offsetIndex);
          }
        }

        // TODO < ------------- Left and Right descriptorL must be alligned?

        if (inputFilesR.isEmpty()) {
          throw new RuntimeException(""); // TODO
        }
        ParquetMetadata metaR = inputFilesR.peek().getFooter();
        long writeLeft = blockMetaDataL.getRowCount();
        while (writeLeft > 0) {
  //         LOG.info("Rewriting input fileRight: {}, remaining fileRight: {}", readerR.getFile(), inputFilesR.size());
          BlockMetaData blockMetaDataR = metaR.getBlocks().get(blockIdR);
          List<ColumnChunkMetaData> chunksR = blockMetaDataR.getColumns();
          long leftInBlock = blockMetaDataR.getRowCount() - writtenInBlock;
          long writeFromBlock = Math.min(writeLeft, leftInBlock);
          for (ColumnChunkMetaData chunkR : chunksR) {
            // If a column is encrypted we simply throw exception. // TODO add that detail to docs
            if (chunkR.isEncrypted()) {
              throw new IOException("Column " + chunkR.getPath().toDotString() + " is encrypted");
            }
            // This column has been pruned.
            ColumnDescriptor descriptorR = descriptorsMapR.get(chunkR.getPath());
            TransParquetFileReader readerR = inputFilesR.peek();
            if (!colReadersR.containsKey(descriptorR)) {
              PageReadStore pageReadStore = readerR.readRowGroup(blockIdR);
              ColumnReadStoreImpl crStore =
                  new ColumnReadStoreImpl(pageReadStore, new DummyGroupConverter(), schema, originalCreatedBy);
              ColumnReader cReader = crStore.getColumnReader(descriptorR);
              colReadersR.put(descriptorR, cReader);
            }
            ColumnReader colReaderR = colReadersR.get(descriptorR);
            buildChunks(descriptorR, chunkR, colReaderR, writer, schemaR, writeFromBlock, numBlocksRewritten);
          }
          writeLeft -= Math.min(writeLeft, blockMetaDataR.getRowCount()); // TODO add exception for empty right schema so we don't fall with exception here?
          writtenInBlock += writeFromBlock;
          if (writeLeft > 0) {
            blockIdR++;
          }
          if (blockIdR == metaR.getBlocks().size()) {
            inputFilesR.poll();
            blockIdR = 0;
            writtenInBlock = 0;
          }
        }


        writer.endBlock();
        numBlocksRewritten++;
      }
    }
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

  private void buildChunks(
      ColumnDescriptor descriptor,
      ColumnChunkMetaData chunk,
      ColumnReader cReader,
      ParquetFileWriter writer,
      MessageType schema,
      long rowsToWrite,
      int numBlocksRewritten)
      throws IOException {

    ParquetProperties.WriterVersion writerVersion = chunk.getEncodingStats().usesV2Pages()
        ? ParquetProperties.WriterVersion.PARQUET_2_0
        : ParquetProperties.WriterVersion.PARQUET_1_0;
    ParquetProperties props =
        ParquetProperties.builder().withWriterVersion(writerVersion).build();
    CodecFactory codecFactory = new CodecFactory(new Configuration(), props.getPageSizeThreshold());
    CompressionCodecFactory.BytesInputCompressor compressor = codecFactory.getCompressor(chunk.getCodec());

    // Create new schema that only has the current column
    MessageType newSchema = newSchema(schema, descriptor);
    ColumnChunkPageWriteStore cPageStore = new ColumnChunkPageWriteStore(
        compressor,
        newSchema,
        props.getAllocator(),
        props.getColumnIndexTruncateLength(),
        props.getPageWriteChecksumEnabled(),
        writer.getEncryptor(),
        numBlocksRewritten);
    ColumnWriteStore cStore = props.newColumnWriteStore(newSchema, cPageStore);
    ColumnWriter cWriter = cStore.getColumnWriter(descriptor);
    Class<?> columnType = descriptor.getPrimitiveType().getPrimitiveTypeName().javaType;

    int rowCount = 0;
    while (rowCount < rowsToWrite) {
//     for (int i = 0; i < rowsToWrite; i++) {
      int rlvl = cReader.getCurrentRepetitionLevel();
      int dlvl = cReader.getCurrentDefinitionLevel();
      if (rlvl == 0) {
        if (rowCount > 0) {
          cStore.endRecord();
        }
        rowCount++;
      }
      if (columnType == Integer.TYPE) {
        cWriter.write(cReader.getInteger(), rlvl, dlvl);
      } else if (columnType == Long.TYPE) {
        cWriter.write(cReader.getLong(), rlvl, dlvl);
      } else if (columnType == Float.TYPE) {
        cWriter.write(cReader.getFloat(), rlvl, dlvl);
      } else if (columnType == Double.TYPE) {
        cWriter.write(cReader.getDouble(), rlvl, dlvl);
      } else if (columnType == Binary.class) {
        cWriter.write(cReader.getBinary(), rlvl, dlvl);
      } else if (columnType == Boolean.TYPE) {
        cWriter.write(cReader.getBoolean(), rlvl, dlvl);
      } else {
          throw new UnsupportedOperationException(
              String.format("Unsupported column java class: %s", columnType.toString()));
      }
      cReader.consume();
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
          return tempField;
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
