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

import static org.apache.parquet.column.ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH;
import static org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnEncryptionSetup;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.IndexCache;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites multiple input files into a single output file.
 * <p>
 * Supported functionality:
 * <ul>
 * <li>Merging multiple files into a single one</li>
 * <li>Applying column transformations</li>
 * <li><i>Joining</i> with extra files with a different schema</li>
 * </ul>
 * <p>
 * Note that the total number of row groups from all input files is preserved in the output file.
 * This may not be optimal if row groups are very small and will not solve small file problems. Instead, it will
 * make it worse to have a large file footer in the output file.
 * <p>
 * <h2>Merging multiple files into a single output files</h2>
 * Use {@link RewriteOptions.Builder}'s constructor or methods to provide <code>inputFiles</code>.
 * Please note the schema of all <code>inputFiles</code> must be the same, otherwise the rewrite will fail.
 * <p>
 * <h2>Applying column transformations</h2>
 * Some supported column transformations: pruning, masking, renaming, encrypting, changing a codec.
 * See {@link RewriteOptions} and {@link RewriteOptions.Builder} for the full list with description.
 * <p>
 * <h2><i>Joining</i> with extra files with a different schema</h2>
 * Use {@link RewriteOptions.Builder}'s constructor or methods to provide <code>inputFilesToJoin</code>.
 * Please note the schema of all <code>inputFilesToJoin</code> must be the same, otherwise the rewrite will fail.
 * Requirements for a <i>joining</i> the main <code>inputFiles</code>(left) and <code>inputFilesToJoin</code>(right):
 * <ul>
 * <li>the number of files might be different on the left and right,</li>
 * <li>the schema of files inside of each group(left/right) must be the same, but those two schemas not necessarily should be equal,</li>
 * <li>the total number of row groups must be the same on the left and right,</li>
 * <li>the total number of rows must be the same on the left and right,</li>
 * <li>the global ordering of rows must be the same on the left and right.</li>
 * </ul>
 */
public class ParquetRewriter implements Closeable {

  // Key to store original writer version in the file key-value metadata
  public static final String ORIGINAL_CREATED_BY_KEY = "original.created.by";
  private static final Logger LOG = LoggerFactory.getLogger(ParquetRewriter.class);
  private final int pageBufferSize = ParquetProperties.DEFAULT_PAGE_SIZE * 2;
  private final byte[] pageBuffer = new byte[pageBufferSize];
  // Configurations for the new file
  private final CompressionCodecName newCodecName;
  private Map<ColumnPath, MaskMode> maskColumns = null;
  private Set<ColumnPath> encryptColumns = null;
  private boolean encryptMode = false;
  private final Map<String, String> extraMetaData;
  // Writer to rewrite the input files
  private final ParquetFileWriter writer;
  // Number of blocks written which is used to keep track of the actual row group ordinal
  private int numBlocksRewritten = 0;
  // Reader and relevant states of the in-processing input file
  private final Queue<TransParquetFileReader> inputFiles = new LinkedList<>();
  private final Queue<TransParquetFileReader> inputFilesToJoin = new LinkedList<>();
  private final MessageType outSchema;
  // The index cache strategy
  private final IndexCache.CacheStrategy indexCacheStrategy;
  private final boolean overwriteInputWithJoinColumns;
  private final InternalFileEncryptor nullColumnEncryptor;
  private final Map<String, String> renamedColumns;

  public ParquetRewriter(RewriteOptions options) throws IOException {
    this.newCodecName = options.getNewCodecName();
    this.indexCacheStrategy = options.getIndexCacheStrategy();
    this.overwriteInputWithJoinColumns = options.getOverwriteInputWithJoinColumns();
    this.renamedColumns = options.getRenameColumns();
    ParquetConfiguration conf = options.getParquetConfiguration();
    this.inputFiles.addAll(getFileReaders(options.getParquetInputFiles(), conf));
    this.inputFilesToJoin.addAll(getFileReaders(options.getParquetInputFilesToJoin(), conf));
    this.outSchema = pruneColumnsInSchema(getSchema(), options.getPruneColumns());
    this.extraMetaData = getExtraMetadata(options);
    ensureSameSchema(inputFiles);
    ensureSameSchema(inputFilesToJoin);
    ensureRowCount();
    ensureRenamingCorrectness(outSchema, renamedColumns);
    OutputFile out = options.getParquetOutputFile();
    LOG.info(
        "Start rewriting {} input file(s) {} to {}",
        inputFiles.size() + inputFilesToJoin.size(),
        Stream.concat(options.getParquetInputFiles().stream(), options.getParquetInputFilesToJoin().stream())
            .collect(Collectors.toList()),
        out);

    if (options.getMaskColumns() != null) {
      this.maskColumns = new HashMap<>();
      for (Map.Entry<String, MaskMode> col : options.getMaskColumns().entrySet()) {
        maskColumns.put(ColumnPath.fromDotString(col.getKey()), col.getValue());
      }
    }

    if (options.getEncryptColumns() != null && options.getFileEncryptionProperties() != null) {
      this.encryptColumns = convertToColumnPaths(options.getEncryptColumns());
      this.encryptMode = true;
    }

    ParquetFileWriter.Mode writerMode = ParquetFileWriter.Mode.CREATE;
    this.writer = new ParquetFileWriter(
        out,
        renamedColumns.isEmpty() ? outSchema : getSchemaWithRenamedColumns(this.outSchema),
        writerMode,
        DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT,
        DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
        DEFAULT_STATISTICS_TRUNCATE_LENGTH,
        ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED,
        options.getFileEncryptionProperties());
    writer.start();
    // column nullification requires a separate encryptor and forcing other columns encryption initialization
    if (options.getFileEncryptionProperties() == null) {
      this.nullColumnEncryptor = null;
    } else {
      this.nullColumnEncryptor = new InternalFileEncryptor(options.getFileEncryptionProperties());
      List<ColumnDescriptor> columns =
          getSchemaWithRenamedColumns(this.outSchema).getColumns();
      for (int i = 0; i < columns.size(); i++) {
        writer.getEncryptor()
            .getColumnSetup(ColumnPath.get(columns.get(i).getPath()), true, i);
      }
    }
  }

  // TODO: Should we mark it as deprecated to encourage the main constructor usage? it is also used only from
  // deprecated classes atm
  // Ctor for legacy CompressionConverter and ColumnMasker
  public ParquetRewriter(
      TransParquetFileReader reader,
      ParquetFileWriter writer,
      ParquetMetadata meta,
      MessageType outSchema,
      String originalCreatedBy,
      CompressionCodecName codecName,
      List<String> maskColumns,
      MaskMode maskMode) {
    this.writer = writer;
    this.outSchema = outSchema;
    this.newCodecName = codecName;
    this.extraMetaData = new HashMap<>(meta.getFileMetaData().getKeyValueMetaData());
    this.extraMetaData.put(
        ORIGINAL_CREATED_BY_KEY,
        originalCreatedBy != null
            ? originalCreatedBy
            : meta.getFileMetaData().getCreatedBy());
    if (maskColumns != null && maskMode != null) {
      this.maskColumns = new HashMap<>();
      for (String col : maskColumns) {
        this.maskColumns.put(ColumnPath.fromDotString(col), maskMode);
      }
    }
    this.inputFiles.add(reader);
    this.indexCacheStrategy = IndexCache.CacheStrategy.NONE;
    this.overwriteInputWithJoinColumns = false;
    this.nullColumnEncryptor = null;
    this.renamedColumns = new HashMap<>();
  }

  private MessageType getSchema() {
    MessageType schemaMain = inputFiles.peek().getFooter().getFileMetaData().getSchema();
    if (inputFilesToJoin.isEmpty()) {
      return schemaMain;
    } else {
      Map<String, Type> fieldNames = new LinkedHashMap<>();
      schemaMain.getFields().forEach(x -> fieldNames.put(x.getName(), x));
      inputFilesToJoin
          .peek()
          .getFooter()
          .getFileMetaData()
          .getSchema()
          .getFields()
          .forEach(x -> {
            if (!fieldNames.containsKey(x.getName())) {
              fieldNames.put(x.getName(), x);
            } else if (overwriteInputWithJoinColumns) {
              LOG.info("Column {} in inputFiles is overwritten by inputFilesToJoin side", x.getName());
              fieldNames.put(x.getName(), x);
            }
          });
      return new MessageType(schemaMain.getName(), new ArrayList<>(fieldNames.values()));
    }
  }

  private MessageType getSchemaWithRenamedColumns(MessageType schema) {
    List<Type> fields = schema.getFields().stream()
        .map(type -> {
          if (!renamedColumns.containsKey(type.getName())) {
            return type;
          } else if (type.isPrimitive()) {
            return new PrimitiveType(
                type.getRepetition(),
                type.asPrimitiveType().getPrimitiveTypeName(),
                renamedColumns.get(type.getName()));
          } else {
            return new GroupType(
                type.getRepetition(),
                renamedColumns.get(type.getName()),
                type.asGroupType().getFields());
          }
        })
        .collect(Collectors.toList());
    return new MessageType(schema.getName(), fields);
  }

  private Map<String, String> getExtraMetadata(RewriteOptions options) {
    List<TransParquetFileReader> allFiles;
    if (options.getIgnoreJoinFilesMetadata()) {
      allFiles = new ArrayList<>(inputFiles);
    } else {
      allFiles = Stream.concat(inputFiles.stream(), inputFilesToJoin.stream())
          .collect(Collectors.toList());
    }
    Map<String, String> result = new HashMap<>();
    result.put(
        ORIGINAL_CREATED_BY_KEY,
        allFiles.stream()
            .map(x -> x.getFooter().getFileMetaData().getCreatedBy())
            .collect(Collectors.toSet())
            .stream()
            .reduce((a, b) -> a + "\n" + b)
            .orElse(""));
    allFiles.forEach(x -> result.putAll(x.getFileMetaData().getKeyValueMetaData()));
    return result;
  }

  private void ensureRowCount() {
    if (!inputFilesToJoin.isEmpty()) {
      List<Long> blocksRowCountsL = inputFiles.stream()
          .flatMap(x -> x.getFooter().getBlocks().stream().map(BlockMetaData::getRowCount))
          .collect(Collectors.toList());
      List<Long> blocksRowCountsR = inputFilesToJoin.stream()
          .flatMap(x -> x.getFooter().getBlocks().stream().map(BlockMetaData::getRowCount))
          .collect(Collectors.toList());
      if (!blocksRowCountsL.equals(blocksRowCountsR)) {
        throw new IllegalArgumentException(
            "The number of rows in each block must match! Left blocks row counts: " + blocksRowCountsL
                + ", right blocks row counts" + blocksRowCountsR + ".");
      }
    }
  }

  private Queue<TransParquetFileReader> getFileReaders(List<InputFile> inputFiles, ParquetConfiguration conf) {
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
          throw new InvalidSchemaException("Input files have different schemas, current file: " + file);
        }
      }
    }
  }

  private void ensureRenamingCorrectness(MessageType schema, Map<String, String> renameMap) {
    Set<String> columns = schema.getFields().stream().map(Type::getName).collect(Collectors.toSet());
    renameMap.forEach((src, dst) -> {
      if (!columns.contains(src)) {
        String msg = String.format("Column to rename '%s' is not found in input files schema", src);
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      } else if (columns.contains(dst)) {
        String msg = String.format("Renamed column target name '%s' is already present in a schema", dst);
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
    });
  }

  @Override
  public void close() throws IOException {
    writer.end(extraMetaData);
  }

  public void processBlocks() throws IOException {
    TransParquetFileReader readerToJoin = null;
    IndexCache indexCacheToJoin = null;
    int blockIdxToJoin = 0;
    List<ColumnDescriptor> outColumns = outSchema.getColumns();

    while (!inputFiles.isEmpty()) {
      TransParquetFileReader reader = inputFiles.poll();
      LOG.info("Rewriting input file: {}, remaining files: {}", reader.getFile(), inputFiles.size());
      ParquetMetadata meta = reader.getFooter();
      Set<ColumnPath> columnPaths = meta.getFileMetaData().getSchema().getColumns().stream()
          .map(x -> ColumnPath.get(x.getPath()))
          .collect(Collectors.toSet());
      IndexCache indexCache = IndexCache.create(reader, columnPaths, indexCacheStrategy, true);

      for (int blockIdx = 0; blockIdx < meta.getBlocks().size(); blockIdx++) {
        BlockMetaData blockMetaData = meta.getBlocks().get(blockIdx);
        writer.startBlock(blockMetaData.getRowCount());
        indexCache.setBlockMetadata(blockMetaData);
        Map<ColumnPath, ColumnChunkMetaData> pathToChunk =
            blockMetaData.getColumns().stream().collect(Collectors.toMap(x -> x.getPath(), x -> x));

        if (!inputFilesToJoin.isEmpty()) {
          if (readerToJoin == null
              || ++blockIdxToJoin
                  == readerToJoin.getFooter().getBlocks().size()) {
            if (readerToJoin != null) readerToJoin.close();
            blockIdxToJoin = 0;
            readerToJoin = inputFilesToJoin.poll();
            Set<ColumnPath> columnPathsToJoin =
                readerToJoin.getFileMetaData().getSchema().getColumns().stream()
                    .map(x -> ColumnPath.get(x.getPath()))
                    .collect(Collectors.toSet());
            if (indexCacheToJoin != null) {
              indexCacheToJoin.clean();
            }
            indexCacheToJoin = IndexCache.create(readerToJoin, columnPathsToJoin, indexCacheStrategy, true);
            indexCacheToJoin.setBlockMetadata(
                readerToJoin.getFooter().getBlocks().get(blockIdxToJoin));
          } else {
            blockIdxToJoin++;
            indexCacheToJoin.setBlockMetadata(
                readerToJoin.getFooter().getBlocks().get(blockIdxToJoin));
          }
        }

        for (int outColumnIdx = 0; outColumnIdx < outColumns.size(); outColumnIdx++) {
          ColumnPath colPath =
              ColumnPath.get(outColumns.get(outColumnIdx).getPath());
          if (readerToJoin != null) {
            Optional<ColumnChunkMetaData> chunkToJoin =
                readerToJoin.getFooter().getBlocks().get(blockIdxToJoin).getColumns().stream()
                    .filter(x -> x.getPath().equals(colPath))
                    .findFirst();
            if (chunkToJoin.isPresent()
                && (overwriteInputWithJoinColumns || !columnPaths.contains(colPath))) {
              processBlock(
                  readerToJoin, blockIdxToJoin, outColumnIdx, indexCacheToJoin, chunkToJoin.get());
            } else {
              processBlock(reader, blockIdx, outColumnIdx, indexCache, pathToChunk.get(colPath));
            }
          } else {
            processBlock(reader, blockIdx, outColumnIdx, indexCache, pathToChunk.get(colPath));
          }
        }

        writer.endBlock();
        indexCache.clean();
        numBlocksRewritten++;
      }

      indexCache.clean();
      LOG.info("Finish rewriting input file: {}", reader.getFile());
      reader.close();
    }
    if (readerToJoin != null) readerToJoin.close();
  }

  private ColumnPath normalizeFieldsInPath(ColumnPath path) {
    if (renamedColumns.isEmpty()) {
      return path;
    } else {
      String[] pathArray = path.toArray();
      pathArray[0] = renamedColumns.getOrDefault(pathArray[0], pathArray[0]);
      return ColumnPath.get(pathArray);
    }
  }

  private PrimitiveType normalizeNameInType(PrimitiveType type) {
    if (renamedColumns.isEmpty()) {
      return type;
    } else {
      return new PrimitiveType(
          type.getRepetition(),
          type.asPrimitiveType().getPrimitiveTypeName(),
          renamedColumns.getOrDefault(type.getName(), type.getName()));
    }
  }

  private void processBlock(
      TransParquetFileReader reader,
      int blockIdx,
      int outColumnIdx,
      IndexCache indexCache,
      ColumnChunkMetaData chunk)
      throws IOException {
    if (chunk.isEncrypted()) {
      throw new IOException("Column " + chunk.getPath().toDotString() + " is already encrypted");
    }

    ColumnChunkMetaData chunkNormalized = chunk;
    if (!renamedColumns.isEmpty()) {
      // Keep an eye if this get stale because of ColumnChunkMetaData change
      chunkNormalized = ColumnChunkMetaData.get(
          normalizeFieldsInPath(chunk.getPath()),
          normalizeNameInType(chunk.getPrimitiveType()),
          chunk.getCodec(),
          chunk.getEncodingStats(),
          chunk.getEncodings(),
          chunk.getStatistics(),
          chunk.getFirstDataPageOffset(),
          chunk.getDictionaryPageOffset(),
          chunk.getValueCount(),
          chunk.getTotalSize(),
          chunk.getTotalUncompressedSize(),
          chunk.getSizeStatistics());
    }

    ColumnDescriptor descriptorOriginal = outSchema.getColumns().get(outColumnIdx);
    ColumnDescriptor descriptorRenamed =
        getSchemaWithRenamedColumns(outSchema).getColumns().get(outColumnIdx);
    BlockMetaData blockMetaData = reader.getFooter().getBlocks().get(blockIdx);
    String originalCreatedBy = reader.getFileMetaData().getCreatedBy();

    reader.setStreamPosition(chunk.getStartingPos());
    CompressionCodecName newCodecName = this.newCodecName == null ? chunk.getCodec() : this.newCodecName;
    boolean encryptColumn = encryptMode && encryptColumns != null && encryptColumns.contains(chunk.getPath());

    if (maskColumns != null && maskColumns.containsKey(chunk.getPath())) {
      // Mask column and compress it again.
      MaskMode maskMode = maskColumns.get(chunk.getPath());
      if (maskMode.equals(MaskMode.NULLIFY)) {
        Type.Repetition repetition =
            descriptorOriginal.getPrimitiveType().getRepetition();
        if (repetition.equals(Type.Repetition.REQUIRED)) {
          throw new IOException("Required column ["
              + descriptorOriginal.getPrimitiveType().getName() + "] cannot be nullified");
        }
        nullifyColumn(
            reader,
            blockIdx,
            descriptorOriginal,
            chunk,
            writer,
            newCodecName,
            encryptColumn,
            originalCreatedBy);
      } else {
        throw new UnsupportedOperationException("Only nullify is supported for now");
      }
    } else if (encryptMode || this.newCodecName != null) {
      // Prepare encryption context
      ColumnChunkEncryptorRunTime columnChunkEncryptorRunTime = null;
      if (encryptMode) {
        columnChunkEncryptorRunTime =
            new ColumnChunkEncryptorRunTime(writer.getEncryptor(), chunk, numBlocksRewritten, outColumnIdx);
      }

      // Translate compression and/or encryption
      writer.startColumn(descriptorRenamed, chunk.getValueCount(), newCodecName);
      processChunk(
          reader,
          blockMetaData.getRowCount(),
          chunk,
          newCodecName,
          columnChunkEncryptorRunTime,
          encryptColumn,
          indexCache.getBloomFilter(chunk),
          indexCache.getColumnIndex(chunk),
          indexCache.getOffsetIndex(chunk),
          originalCreatedBy);
      writer.endColumn();
    } else {
      // Nothing changed, simply copy the binary data.
      BloomFilter bloomFilter = indexCache.getBloomFilter(chunk);
      ColumnIndex columnIndex = indexCache.getColumnIndex(chunk);
      OffsetIndex offsetIndex = indexCache.getOffsetIndex(chunk);
      writer.appendColumnChunk(
          descriptorRenamed, reader.getStream(), chunkNormalized, bloomFilter, columnIndex, offsetIndex);
    }
  }

  private void processChunk(
      TransParquetFileReader reader,
      long blockRowCount,
      ColumnChunkMetaData chunk,
      CompressionCodecName newCodecName,
      ColumnChunkEncryptorRunTime columnChunkEncryptorRunTime,
      boolean encryptColumn,
      BloomFilter bloomFilter,
      ColumnIndex columnIndex,
      OffsetIndex offsetIndex,
      String originalCreatedBy)
      throws IOException {
    CompressionCodecFactory codecFactory = HadoopCodecs.newFactory(0);
    CompressionCodecFactory.BytesInputDecompressor decompressor = null;
    CompressionCodecFactory.BytesInputCompressor compressor = null;
    if (!newCodecName.equals(chunk.getCodec())) {
      // Re-compress only if a different codec has been specified
      decompressor = codecFactory.getDecompressor(chunk.getCodec());
      compressor = codecFactory.getCompressor(newCodecName);
    }

    // EncryptorRunTime is only provided when encryption is required
    BlockCipher.Encryptor metaEncryptor = null;
    BlockCipher.Encryptor dataEncryptor = null;
    byte[] dictPageAAD = null;
    byte[] dataPageAAD = null;
    byte[] dictPageHeaderAAD = null;
    byte[] dataPageHeaderAAD = null;
    if (columnChunkEncryptorRunTime != null) {
      metaEncryptor = columnChunkEncryptorRunTime.getMetaDataEncryptor();
      dataEncryptor = columnChunkEncryptorRunTime.getDataEncryptor();
      dictPageAAD = columnChunkEncryptorRunTime.getDictPageAAD();
      dataPageAAD = columnChunkEncryptorRunTime.getDataPageAAD();
      dictPageHeaderAAD = columnChunkEncryptorRunTime.getDictPageHeaderAAD();
      dataPageHeaderAAD = columnChunkEncryptorRunTime.getDataPageHeaderAAD();
    }

    if (bloomFilter != null) {
      writer.addBloomFilter(normalizeFieldsInPath(chunk.getPath()).toDotString(), bloomFilter);
    }

    reader.setStreamPosition(chunk.getStartingPos());
    DictionaryPage dictionaryPage = null;
    long readValues = 0L;
    long readRows = 0L;
    Statistics<?> statistics = null;
    boolean isColumnStatisticsMalformed = false;
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    int pageOrdinal = 0;
    long totalChunkValues = chunk.getValueCount();
    while (readValues < totalChunkValues) {
      PageHeader pageHeader = reader.readPageHeader();
      int compressedPageSize = pageHeader.getCompressed_page_size();
      byte[] pageLoad;
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          if (dictionaryPage != null) {
            throw new IOException("has more than one dictionary page in column chunk: " + chunk);
          }
          // No quickUpdatePageAAD needed for dictionary page
          DictionaryPageHeader dictPageHeader = pageHeader.dictionary_page_header;
          pageLoad = processPageLoad(
              reader,
              true,
              compressor,
              decompressor,
              pageHeader.getCompressed_page_size(),
              pageHeader.getUncompressed_page_size(),
              encryptColumn,
              dataEncryptor,
              dictPageAAD);
          dictionaryPage = new DictionaryPage(
              BytesInput.from(pageLoad),
              pageHeader.getUncompressed_page_size(),
              dictPageHeader.getNum_values(),
              converter.getEncoding(dictPageHeader.getEncoding()));
          writer.writeDictionaryPage(dictionaryPage, metaEncryptor, dictPageHeaderAAD);
          break;
        case DATA_PAGE:
          if (encryptColumn) {
            AesCipher.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
            AesCipher.quickUpdatePageAAD(dataPageAAD, pageOrdinal);
          }
          DataPageHeader headerV1 = pageHeader.data_page_header;
          pageLoad = processPageLoad(
              reader,
              true,
              compressor,
              decompressor,
              pageHeader.getCompressed_page_size(),
              pageHeader.getUncompressed_page_size(),
              encryptColumn,
              dataEncryptor,
              dataPageAAD);
          statistics = convertStatistics(
              originalCreatedBy,
              normalizeNameInType(chunk.getPrimitiveType()),
              headerV1.getStatistics(),
              columnIndex,
              pageOrdinal,
              converter);
          if (statistics == null) {
            // Reach here means both the columnIndex and the page header statistics are null
            isColumnStatisticsMalformed = true;
          } else {
            Preconditions.checkState(
                !isColumnStatisticsMalformed,
                "Detected mixed null page statistics and non-null page statistics");
          }
          readValues += headerV1.getNum_values();
          if (offsetIndex != null) {
            long rowCount = 1
                + offsetIndex.getLastRowIndex(pageOrdinal, blockRowCount)
                - offsetIndex.getFirstRowIndex(pageOrdinal);
            readRows += rowCount;
            writer.writeDataPage(
                toIntWithCheck(headerV1.getNum_values()),
                pageHeader.getUncompressed_page_size(),
                BytesInput.from(pageLoad),
                statistics,
                toIntWithCheck(rowCount),
                converter.getEncoding(headerV1.getRepetition_level_encoding()),
                converter.getEncoding(headerV1.getDefinition_level_encoding()),
                converter.getEncoding(headerV1.getEncoding()),
                metaEncryptor,
                dataPageHeaderAAD);
          } else {
            writer.writeDataPage(
                toIntWithCheck(headerV1.getNum_values()),
                pageHeader.getUncompressed_page_size(),
                BytesInput.from(pageLoad),
                statistics,
                converter.getEncoding(headerV1.getRepetition_level_encoding()),
                converter.getEncoding(headerV1.getDefinition_level_encoding()),
                converter.getEncoding(headerV1.getEncoding()),
                metaEncryptor,
                dataPageHeaderAAD);
          }
          pageOrdinal++;
          break;
        case DATA_PAGE_V2:
          if (encryptColumn) {
            AesCipher.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
            AesCipher.quickUpdatePageAAD(dataPageAAD, pageOrdinal);
          }
          DataPageHeaderV2 headerV2 = pageHeader.data_page_header_v2;
          int rlLength = headerV2.getRepetition_levels_byte_length();
          BytesInput rlLevels = readBlockAllocate(rlLength, reader);
          int dlLength = headerV2.getDefinition_levels_byte_length();
          BytesInput dlLevels = readBlockAllocate(dlLength, reader);
          int payLoadLength = pageHeader.getCompressed_page_size() - rlLength - dlLength;
          int rawDataLength = pageHeader.getUncompressed_page_size() - rlLength - dlLength;
          pageLoad = processPageLoad(
              reader,
              headerV2.is_compressed,
              compressor,
              decompressor,
              payLoadLength,
              rawDataLength,
              encryptColumn,
              dataEncryptor,
              dataPageAAD);
          statistics = convertStatistics(
              originalCreatedBy,
              normalizeNameInType(chunk.getPrimitiveType()),
              headerV2.getStatistics(),
              columnIndex,
              pageOrdinal,
              converter);
          if (statistics == null) {
            // Reach here means both the columnIndex and the page header statistics are null
            isColumnStatisticsMalformed = true;
          } else {
            Preconditions.checkState(
                !isColumnStatisticsMalformed,
                "Detected mixed null page statistics and non-null page statistics");
          }
          readValues += headerV2.getNum_values();
          readRows += headerV2.getNum_rows();
          writer.writeDataPageV2(
              headerV2.getNum_rows(),
              headerV2.getNum_nulls(),
              headerV2.getNum_values(),
              rlLevels,
              dlLevels,
              converter.getEncoding(headerV2.getEncoding()),
              BytesInput.from(pageLoad),
              rawDataLength,
              statistics,
              metaEncryptor,
              dataPageHeaderAAD);
          pageOrdinal++;
          break;
        default:
          LOG.debug("skipping page of type {} of size {}", pageHeader.getType(), compressedPageSize);
          break;
      }
    }

    Preconditions.checkState(
        readRows == 0 || readRows == blockRowCount,
        "Read row count: %s not match with block total row count: %s",
        readRows,
        blockRowCount);

    if (isColumnStatisticsMalformed) {
      // All the column statistics are invalid, so we need to overwrite the column statistics
      writer.invalidateStatistics(chunk.getStatistics());
    }
  }

  private Statistics<?> convertStatistics(
      String createdBy,
      PrimitiveType type,
      org.apache.parquet.format.Statistics pageStatistics,
      ColumnIndex columnIndex,
      int pageIndex,
      ParquetMetadataConverter converter)
      throws IOException {
    if (columnIndex != null) {
      if (columnIndex.getNullPages() == null) {
        throw new IOException(
            "columnIndex has null variable 'nullPages' which indicates corrupted data for type: "
                + type.getName());
      }
      if (pageIndex > columnIndex.getNullPages().size()) {
        throw new IOException(
            "There are more pages " + pageIndex + " found in the column than in the columnIndex "
                + columnIndex.getNullPages().size());
      }
      org.apache.parquet.column.statistics.Statistics.Builder statsBuilder =
          org.apache.parquet.column.statistics.Statistics.getBuilderForReading(type);
      statsBuilder.withNumNulls(columnIndex.getNullCounts().get(pageIndex));

      if (!columnIndex.getNullPages().get(pageIndex)) {
        statsBuilder.withMin(
            columnIndex.getMinValues().get(pageIndex).array().clone());
        statsBuilder.withMax(
            columnIndex.getMaxValues().get(pageIndex).array().clone());
      }
      return statsBuilder.build();
    } else if (pageStatistics != null) {
      return converter.fromParquetStatistics(createdBy, pageStatistics, type);
    } else {
      return null;
    }
  }

  private byte[] processPageLoad(
      TransParquetFileReader reader,
      boolean isCompressed,
      CompressionCodecFactory.BytesInputCompressor compressor,
      CompressionCodecFactory.BytesInputDecompressor decompressor,
      int payloadLength,
      int rawDataLength,
      boolean encrypt,
      BlockCipher.Encryptor dataEncryptor,
      byte[] AAD)
      throws IOException {
    BytesInput data = readBlock(payloadLength, reader);

    // recompress page load
    if (compressor != null) {
      if (isCompressed) {
        data = decompressor.decompress(data, rawDataLength);
      }
      data = compressor.compress(data);
    }

    if (!encrypt) {
      return data.toByteArray();
    }

    // encrypt page load
    return dataEncryptor.encrypt(data.toByteArray(), AAD);
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
    if ((int) size != size) {
      throw new ParquetEncodingException("size is bigger than " + Integer.MAX_VALUE + " bytes: " + size);
    }
    return (int) size;
  }

  // We have to rewrite getPaths because MessageType only get level 0 paths
  private void getPaths(GroupType schema, List<String> paths, String parent) {
    List<Type> fields = schema.getFields();
    String prefix = (parent == null) ? "" : parent + ".";
    for (Type field : fields) {
      paths.add(prefix + field.getName());
      if (field instanceof GroupType) {
        getPaths(field.asGroupType(), paths, prefix + field.getName());
      }
    }
  }

  private MessageType pruneColumnsInSchema(MessageType schema, List<String> pruneColumns) {
    if (pruneColumns == null || pruneColumns.isEmpty()) {
      return schema;
    } else {
      List<String> paths = new ArrayList<>();
      getPaths(schema, paths, null);
      for (String col : pruneColumns) {
        if (!paths.contains(col)) {
          LOG.warn("Input column name {} doesn't show up in the schema", col);
        }
      }
      Set<ColumnPath> prunePaths = convertToColumnPaths(pruneColumns);

      List<Type> fields = schema.getFields();
      List<String> currentPath = new ArrayList<>();
      List<Type> prunedFields = pruneColumnsInFields(fields, currentPath, prunePaths);
      return new MessageType(schema.getName(), prunedFields);
    }
  }

  private List<Type> pruneColumnsInFields(List<Type> fields, List<String> currentPath, Set<ColumnPath> prunePaths) {
    List<Type> prunedFields = new ArrayList<>();
    for (Type childField : fields) {
      Type prunedChildField = pruneColumnsInField(childField, currentPath, prunePaths);
      if (prunedChildField != null) {
        prunedFields.add(prunedChildField);
      }
    }
    return prunedFields;
  }

  private Type pruneColumnsInField(Type field, List<String> currentPath, Set<ColumnPath> prunePaths) {
    String fieldName = field.getName();
    currentPath.add(fieldName);
    ColumnPath path = ColumnPath.get(currentPath.toArray(new String[0]));
    Type prunedField = null;
    if (!prunePaths.contains(path)) {
      if (field.isPrimitive()) {
        prunedField = field;
      } else {
        List<Type> childFields = ((GroupType) field).getFields();
        List<Type> prunedFields = pruneColumnsInFields(childFields, currentPath, prunePaths);
        if (!prunedFields.isEmpty()) {
          prunedField = ((GroupType) field).withNewFields(prunedFields);
        }
      }
    }

    currentPath.remove(currentPath.size() - 1);
    return prunedField;
  }

  private Set<ColumnPath> convertToColumnPaths(List<String> cols) {
    Set<ColumnPath> prunePaths = new HashSet<>();
    for (String col : cols) {
      prunePaths.add(ColumnPath.fromDotString(col));
    }
    return prunePaths;
  }

  private void nullifyColumn(
      TransParquetFileReader reader,
      int blockIndex,
      ColumnDescriptor descriptor,
      ColumnChunkMetaData chunk,
      ParquetFileWriter writer,
      CompressionCodecName newCodecName,
      boolean encryptColumn,
      String originalCreatedBy)
      throws IOException {
    if (encryptColumn) {
      Preconditions.checkArgument(writer.getEncryptor() != null, "Missing encryptor");
    }

    long totalChunkValues = chunk.getValueCount();
    int dMax = descriptor.getMaxDefinitionLevel();
    PageReadStore pageReadStore = reader.readRowGroup(blockIndex);
    ColumnReadStoreImpl crStore =
        new ColumnReadStoreImpl(pageReadStore, new DummyGroupConverter(), outSchema, originalCreatedBy);
    ColumnReader cReader = crStore.getColumnReader(descriptor);

    ParquetProperties.WriterVersion writerVersion = chunk.getEncodingStats().usesV2Pages()
        ? ParquetProperties.WriterVersion.PARQUET_2_0
        : ParquetProperties.WriterVersion.PARQUET_1_0;
    ParquetProperties props =
        ParquetProperties.builder().withWriterVersion(writerVersion).build();
    CodecFactory codecFactory = new CodecFactory(new Configuration(), props.getPageSizeThreshold());
    CompressionCodecFactory.BytesInputCompressor compressor = codecFactory.getCompressor(newCodecName);

    // Create new schema that only has the current column
    MessageType newSchema = getSchemaWithRenamedColumns(newSchema(outSchema, descriptor));
    ColumnChunkPageWriteStore cPageStore = new ColumnChunkPageWriteStore(
        compressor,
        newSchema,
        props.getAllocator(),
        props.getColumnIndexTruncateLength(),
        props.getPageWriteChecksumEnabled(),
        nullColumnEncryptor,
        numBlocksRewritten);
    ColumnWriteStore cStore = props.newColumnWriteStore(newSchema, cPageStore);
    ColumnWriter cWriter = cStore.getColumnWriter(descriptor);

    for (int i = 0; i < totalChunkValues; i++) {
      int rlvl = cReader.getCurrentRepetitionLevel();
      int dlvl = cReader.getCurrentDefinitionLevel();
      if (dlvl == dMax) {
        // since we checked ether optional or repeated, dlvl should be > 0
        if (dlvl == 0) {
          throw new IOException("definition level is detected to be 0 for column "
              + chunk.getPath().toDotString() + " to be nullified");
        }
        // we just write one null for the whole list at the top level,
        // instead of nullify the elements in the list one by one
        if (rlvl == 0) {
          cWriter.writeNull(rlvl, dlvl - 1);
        }
      } else {
        cWriter.writeNull(rlvl, dlvl);
      }
      cStore.endRecord();
    }

    pageReadStore.close();
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

  private static class ColumnChunkEncryptorRunTime {
    private final InternalColumnEncryptionSetup colEncrSetup;
    private final BlockCipher.Encryptor dataEncryptor;
    private final BlockCipher.Encryptor metaDataEncryptor;
    private final byte[] fileAAD;

    private final byte[] dataPageHeaderAAD;
    private final byte[] dataPageAAD;
    private final byte[] dictPageHeaderAAD;
    private final byte[] dictPageAAD;

    public ColumnChunkEncryptorRunTime(
        InternalFileEncryptor fileEncryptor, ColumnChunkMetaData chunk, int blockId, int columnId)
        throws IOException {
      Preconditions.checkArgument(
          fileEncryptor != null, "FileEncryptor is required to create ColumnChunkEncryptorRunTime");

      this.colEncrSetup = fileEncryptor.getColumnSetup(chunk.getPath(), true, columnId);
      this.dataEncryptor = colEncrSetup.getDataEncryptor();
      this.metaDataEncryptor = colEncrSetup.getMetaDataEncryptor();

      this.fileAAD = fileEncryptor.getFileAAD();
      if (colEncrSetup != null && colEncrSetup.isEncrypted()) {
        this.dataPageHeaderAAD = createAAD(ModuleType.DataPageHeader, blockId, columnId);
        this.dataPageAAD = createAAD(ModuleType.DataPage, blockId, columnId);
        this.dictPageHeaderAAD = createAAD(ModuleType.DictionaryPageHeader, blockId, columnId);
        this.dictPageAAD = createAAD(ModuleType.DictionaryPage, blockId, columnId);
      } else {
        this.dataPageHeaderAAD = null;
        this.dataPageAAD = null;
        this.dictPageHeaderAAD = null;
        this.dictPageAAD = null;
      }
    }

    private byte[] createAAD(ModuleType moduleType, int blockId, int columnId) {
      return AesCipher.createModuleAAD(fileAAD, moduleType, blockId, columnId, 0);
    }

    public BlockCipher.Encryptor getDataEncryptor() {
      return this.dataEncryptor;
    }

    public BlockCipher.Encryptor getMetaDataEncryptor() {
      return this.metaDataEncryptor;
    }

    public byte[] getDataPageHeaderAAD() {
      return this.dataPageHeaderAAD;
    }

    public byte[] getDataPageAAD() {
      return this.dataPageAAD;
    }

    public byte[] getDictPageHeaderAAD() {
      return this.dictPageHeaderAAD;
    }

    public byte[] getDictPageAAD() {
      return this.dictPageAAD;
    }
  }
}
