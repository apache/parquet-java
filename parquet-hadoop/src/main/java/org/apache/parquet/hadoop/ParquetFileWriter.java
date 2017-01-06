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

import static org.apache.parquet.format.Util.writeFileMetaData;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.Strings;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.GlobalMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.TypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal implementation of the Parquet file writer as a block container
 *
 * @author Julien Le Dem
 *
 */
public class ParquetFileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileWriter.class);

  private static ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();

  public static final String PARQUET_METADATA_FILE = "_metadata";
  public static final String PARQUET_COMMON_METADATA_FILE = "_common_metadata";
  public static final byte[] MAGIC = "PAR1".getBytes(Charset.forName("ASCII"));
  public static final int CURRENT_VERSION = 1;

  // need to supply a buffer size when setting block size. this is the default
  // for hadoop 1 to present. copying it avoids loading DFSConfigKeys.
  private static final int DFS_BUFFER_SIZE_DEFAULT = 4096;

  // visible for testing
  static final Set<String> BLOCK_FS_SCHEMES = new HashSet<String>();
  static {
    BLOCK_FS_SCHEMES.add("hdfs");
    BLOCK_FS_SCHEMES.add("webhdfs");
    BLOCK_FS_SCHEMES.add("viewfs");
  }

  private static boolean supportsBlockSize(FileSystem fs) {
    return BLOCK_FS_SCHEMES.contains(fs.getUri().getScheme());
  }

  // File creation modes
  public static enum Mode {
    CREATE,
    OVERWRITE
  }

  private final MessageType schema;
  private final FSDataOutputStream out;
  private final AlignmentStrategy alignment;

  // file data
  private List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

  // row group data
  private BlockMetaData currentBlock; // appended to by endColumn

  // row group data set at the start of a row group
  private long currentRecordCount; // set in startBlock

  // column chunk data accumulated as pages are written
  private EncodingStats.Builder encodingStatsBuilder;
  private Set<Encoding> currentEncodings;
  private long uncompressedLength;
  private long compressedLength;
  private Statistics currentStatistics; // accumulated in writePage(s)

  // column chunk data set at the start of a column
  private CompressionCodecName currentChunkCodec; // set in startColumn
  private ColumnPath currentChunkPath;            // set in startColumn
  private PrimitiveTypeName currentChunkType;     // set in startColumn
  private long currentChunkValueCount;            // set in startColumn
  private long currentChunkFirstDataPage;         // set in startColumn (out.pos())
  private long currentChunkDictionaryPageOffset;  // set in writeDictionaryPage

  /**
   * Captures the order in which methods should be called
   *
   * @author Julien Le Dem
   *
   */
  private enum STATE {
    NOT_STARTED {
      STATE start() {
        return STARTED;
      }
    },
    STARTED {
      STATE startBlock() {
        return BLOCK;
      }
      STATE end() {
        return ENDED;
      }
    },
    BLOCK  {
      STATE startColumn() {
        return COLUMN;
      }
      STATE endBlock() {
        return STARTED;
      }
    },
    COLUMN {
      STATE endColumn() {
        return BLOCK;
      };
      STATE write() {
        return this;
      }
    },
    ENDED;

    STATE start() throws IOException { return error(); }
    STATE startBlock() throws IOException { return error(); }
    STATE startColumn() throws IOException { return error(); }
    STATE write() throws IOException { return error(); }
    STATE endColumn() throws IOException { return error(); }
    STATE endBlock() throws IOException { return error(); }
    STATE end() throws IOException { return error(); }

    private final STATE error() throws IOException {
      throw new IOException("The file being written is in an invalid state. Probably caused by an error thrown previously. Current state: " + this.name());
    }
  }

  private STATE state = STATE.NOT_STARTED;

  /**
   * @param configuration Hadoop configuration
   * @param schema the schema of the data
   * @param file the file to write to
   * @throws IOException if the file can not be created
   */
  public ParquetFileWriter(Configuration configuration, MessageType schema,
      Path file) throws IOException {
    this(configuration, schema, file, Mode.CREATE, DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT);
  }

  /**
   * @param configuration Hadoop configuration
   * @param schema the schema of the data
   * @param file the file to write to
   * @param mode file creation mode
   * @throws IOException if the file can not be created
   */
  public ParquetFileWriter(Configuration configuration, MessageType schema,
                           Path file, Mode mode) throws IOException {
    this(configuration, schema, file, mode, DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT);
  }

  /**
   * @param configuration Hadoop configuration
   * @param schema the schema of the data
   * @param file the file to write to
   * @param mode file creation mode
   * @param rowGroupSize the row group size
   * @throws IOException if the file can not be created
   */
  public ParquetFileWriter(Configuration configuration, MessageType schema,
                           Path file, Mode mode, long rowGroupSize,
                           int maxPaddingSize)
      throws IOException {
    TypeUtil.checkValidWriteSchema(schema);
    this.schema = schema;
    FileSystem fs = file.getFileSystem(configuration);
    boolean overwriteFlag = (mode == Mode.OVERWRITE);

    if (supportsBlockSize(fs)) {
      // use the default block size, unless row group size is larger
      long dfsBlockSize = Math.max(fs.getDefaultBlockSize(file), rowGroupSize);

      this.alignment = PaddingAlignment.get(
          dfsBlockSize, rowGroupSize, maxPaddingSize);
      this.out = fs.create(file, overwriteFlag, DFS_BUFFER_SIZE_DEFAULT,
          fs.getDefaultReplication(file), dfsBlockSize);

    } else {
      this.alignment = NoAlignment.get(rowGroupSize);
      this.out = fs.create(file, overwriteFlag);
    }

    this.encodingStatsBuilder = new EncodingStats.Builder();
  }

  /**
   * FOR TESTING ONLY.
   *
   * @param configuration Hadoop configuration
   * @param schema the schema of the data
   * @param file the file to write to
   * @param rowAndBlockSize the row group size
   * @throws IOException if the file can not be created
   */
  ParquetFileWriter(Configuration configuration, MessageType schema,
                    Path file, long rowAndBlockSize, int maxPaddingSize)
      throws IOException {
    FileSystem fs = file.getFileSystem(configuration);
    this.schema = schema;
    this.alignment = PaddingAlignment.get(
        rowAndBlockSize, rowAndBlockSize, maxPaddingSize);
    this.out = fs.create(file, true, DFS_BUFFER_SIZE_DEFAULT,
        fs.getDefaultReplication(file), rowAndBlockSize);
    this.encodingStatsBuilder = new EncodingStats.Builder();
  }

  /**
   * start the file
   * @throws IOException
   */
  public void start() throws IOException {
    state = state.start();
    LOG.debug("{}: start", out.getPos());
    out.write(MAGIC);
  }

  /**
   * start a block
   * @param recordCount the record count in this block
   * @throws IOException
   */
  public void startBlock(long recordCount) throws IOException {
    state = state.startBlock();
    LOG.debug("{}: start block", out.getPos());
//    out.write(MAGIC); // TODO: add a magic delimiter

    alignment.alignForRowGroup(out);

    currentBlock = new BlockMetaData();
    currentRecordCount = recordCount;
  }

  /**
   * start a column inside a block
   * @param descriptor the column descriptor
   * @param valueCount the value count in this column
   * @param compressionCodecName
   * @throws IOException
   */
  public void startColumn(ColumnDescriptor descriptor,
                          long valueCount,
                          CompressionCodecName compressionCodecName) throws IOException {
    state = state.startColumn();
    encodingStatsBuilder.clear();
    currentEncodings = new HashSet<Encoding>();
    currentChunkPath = ColumnPath.get(descriptor.getPath());
    currentChunkType = descriptor.getType();
    currentChunkCodec = compressionCodecName;
    currentChunkValueCount = valueCount;
    currentChunkFirstDataPage = out.getPos();
    compressedLength = 0;
    uncompressedLength = 0;
    // need to know what type of stats to initialize to
    // better way to do this?
    currentStatistics = Statistics.getStatsBasedOnType(currentChunkType);
  }

  /**
   * writes a dictionary page page
   * @param dictionaryPage the dictionary page
   */
  public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
    state = state.write();
    LOG.debug("{}: write dictionary page: {} values", out.getPos(), dictionaryPage.getDictionarySize());
    currentChunkDictionaryPageOffset = out.getPos();
    int uncompressedSize = dictionaryPage.getUncompressedSize();
    int compressedPageSize = (int)dictionaryPage.getBytes().size(); // TODO: fix casts
    metadataConverter.writeDictionaryPageHeader(
        uncompressedSize,
        compressedPageSize,
        dictionaryPage.getDictionarySize(),
        dictionaryPage.getEncoding(),
        out);
    long headerSize = out.getPos() - currentChunkDictionaryPageOffset;
    this.uncompressedLength += uncompressedSize + headerSize;
    this.compressedLength += compressedPageSize + headerSize;
    LOG.debug("{}: write dictionary page content {}", out.getPos(), compressedPageSize);
    dictionaryPage.getBytes().writeAllTo(out);
    encodingStatsBuilder.addDictEncoding(dictionaryPage.getEncoding());
    currentEncodings.add(dictionaryPage.getEncoding());
  }


  /**
   * writes a single page
   * @param valueCount count of values
   * @param uncompressedPageSize the size of the data once uncompressed
   * @param bytes the compressed data for the page without header
   * @param rlEncoding encoding of the repetition level
   * @param dlEncoding encoding of the definition level
   * @param valuesEncoding encoding of values
   */
  @Deprecated
  public void writeDataPage(
      int valueCount, int uncompressedPageSize,
      BytesInput bytes,
      Encoding rlEncoding,
      Encoding dlEncoding,
      Encoding valuesEncoding) throws IOException {
    state = state.write();
    long beforeHeader = out.getPos();
    LOG.debug("{}: write data page: {} values", beforeHeader, valueCount);
    int compressedPageSize = (int)bytes.size();
    metadataConverter.writeDataPageHeader(
        uncompressedPageSize, compressedPageSize,
        valueCount,
        rlEncoding,
        dlEncoding,
        valuesEncoding,
        out);
    long headerSize = out.getPos() - beforeHeader;
    this.uncompressedLength += uncompressedPageSize + headerSize;
    this.compressedLength += compressedPageSize + headerSize;
    LOG.debug("{}: write data page content {}", out.getPos(), compressedPageSize);
    bytes.writeAllTo(out);
    encodingStatsBuilder.addDataEncoding(valuesEncoding);
    currentEncodings.add(rlEncoding);
    currentEncodings.add(dlEncoding);
    currentEncodings.add(valuesEncoding);
  }

  /**
   * writes a single page
   * @param valueCount count of values
   * @param uncompressedPageSize the size of the data once uncompressed
   * @param bytes the compressed data for the page without header
   * @param rlEncoding encoding of the repetition level
   * @param dlEncoding encoding of the definition level
   * @param valuesEncoding encoding of values
   */
  public void writeDataPage(
      int valueCount, int uncompressedPageSize,
      BytesInput bytes,
      Statistics statistics,
      Encoding rlEncoding,
      Encoding dlEncoding,
      Encoding valuesEncoding) throws IOException {
    state = state.write();
    long beforeHeader = out.getPos();
    LOG.debug("{}: write data page: {} values", beforeHeader, valueCount);
    int compressedPageSize = (int)bytes.size();
    metadataConverter.writeDataPageHeader(
        uncompressedPageSize, compressedPageSize,
        valueCount,
        statistics,
        rlEncoding,
        dlEncoding,
        valuesEncoding,
        out);
    long headerSize = out.getPos() - beforeHeader;
    this.uncompressedLength += uncompressedPageSize + headerSize;
    this.compressedLength += compressedPageSize + headerSize;
    LOG.debug("{}: write data page content {}", out.getPos(), compressedPageSize);
    bytes.writeAllTo(out);
    currentStatistics.mergeStatistics(statistics);
    encodingStatsBuilder.addDataEncoding(valuesEncoding);
    currentEncodings.add(rlEncoding);
    currentEncodings.add(dlEncoding);
    currentEncodings.add(valuesEncoding);
  }

  /**
   * writes a number of pages at once
   * @param bytes bytes to be written including page headers
   * @param uncompressedTotalPageSize total uncompressed size (without page headers)
   * @param compressedTotalPageSize total compressed size (without page headers)
   * @throws IOException
   */
  void writeDataPages(BytesInput bytes,
                      long uncompressedTotalPageSize,
                      long compressedTotalPageSize,
                      Statistics totalStats,
                      Set<Encoding> rlEncodings,
                      Set<Encoding> dlEncodings,
                      List<Encoding> dataEncodings) throws IOException {
    state = state.write();
    LOG.debug("{}: write data pages", out.getPos());
    long headersSize = bytes.size() - compressedTotalPageSize;
    this.uncompressedLength += uncompressedTotalPageSize + headersSize;
    this.compressedLength += compressedTotalPageSize + headersSize;
    LOG.debug("{}: write data pages content", out.getPos());
    bytes.writeAllTo(out);
    encodingStatsBuilder.addDataEncodings(dataEncodings);
    if (rlEncodings.isEmpty()) {
      encodingStatsBuilder.withV2Pages();
    }
    currentEncodings.addAll(rlEncodings);
    currentEncodings.addAll(dlEncodings);
    currentEncodings.addAll(dataEncodings);
    currentStatistics = totalStats;
  }

  /**
   * end a column (once all rep, def and data have been written)
   * @throws IOException
   */
  public void endColumn() throws IOException {
    state = state.endColumn();
    LOG.debug("{}: end column", out.getPos());
    currentBlock.addColumn(ColumnChunkMetaData.get(
        currentChunkPath,
        currentChunkType,
        currentChunkCodec,
        encodingStatsBuilder.build(),
        currentEncodings,
        currentStatistics,
        currentChunkFirstDataPage,
        currentChunkDictionaryPageOffset,
        currentChunkValueCount,
        compressedLength,
        uncompressedLength));
    this.currentBlock.setTotalByteSize(currentBlock.getTotalByteSize() + uncompressedLength);
    this.uncompressedLength = 0;
    this.compressedLength = 0;
  }

  /**
   * ends a block once all column chunks have been written
   * @throws IOException
   */
  public void endBlock() throws IOException {
    state = state.endBlock();
    LOG.debug("{}: end block", out.getPos());
    currentBlock.setRowCount(currentRecordCount);
    blocks.add(currentBlock);
    currentBlock = null;
  }

  public void appendFile(Configuration conf, Path file) throws IOException {
    ParquetFileReader.open(conf, file).appendTo(this);
  }

  public void appendRowGroups(FSDataInputStream file,
                              List<BlockMetaData> rowGroups,
                              boolean dropColumns) throws IOException {
    appendRowGroups(HadoopStreams.wrap(file), rowGroups, dropColumns);
  }

  public void appendRowGroups(SeekableInputStream file,
                              List<BlockMetaData> rowGroups,
                              boolean dropColumns) throws IOException {
    for (BlockMetaData block : rowGroups) {
      appendRowGroup(file, block, dropColumns);
    }
  }

  public void appendRowGroup(FSDataInputStream from, BlockMetaData rowGroup,
                             boolean dropColumns) throws IOException {
    appendRowGroup(from, rowGroup, dropColumns);
  }

  public void appendRowGroup(SeekableInputStream from, BlockMetaData rowGroup,
    boolean dropColumns) throws IOException {
    startBlock(rowGroup.getRowCount());

    Map<String, ColumnChunkMetaData> columnsToCopy =
        new HashMap<String, ColumnChunkMetaData>();
    for (ColumnChunkMetaData chunk : rowGroup.getColumns()) {
      columnsToCopy.put(chunk.getPath().toDotString(), chunk);
    }

    List<ColumnChunkMetaData> columnsInOrder =
        new ArrayList<ColumnChunkMetaData>();

    for (ColumnDescriptor descriptor : schema.getColumns()) {
      String path = ColumnPath.get(descriptor.getPath()).toDotString();
      ColumnChunkMetaData chunk = columnsToCopy.remove(path);
      if (chunk != null) {
        columnsInOrder.add(chunk);
      } else {
        throw new IllegalArgumentException(String.format(
            "Missing column '%s', cannot copy row group: %s", path, rowGroup));
      }
    }

    // complain if some columns would be dropped and that's not okay
    if (!dropColumns && !columnsToCopy.isEmpty()) {
      throw new IllegalArgumentException(String.format(
          "Columns cannot be copied (missing from target schema): %s",
          Strings.join(columnsToCopy.keySet(), ", ")));
    }

    // copy the data for all chunks
    long start = -1;
    long length = 0;
    long blockCompressedSize = 0;
    for (int i = 0; i < columnsInOrder.size(); i += 1) {
      ColumnChunkMetaData chunk = columnsInOrder.get(i);

      // get this chunk's start position in the new file
      long newChunkStart = out.getPos() + length;

      // add this chunk to be copied with any previous chunks
      if (start < 0) {
        // no previous chunk included, start at this chunk's starting pos
        start = chunk.getStartingPos();
      }
      length += chunk.getTotalSize();

      if ((i + 1) == columnsInOrder.size() ||
          columnsInOrder.get(i + 1).getStartingPos() != (start + length)) {
        // not contiguous. do the copy now.
        copy(from, out, start, length);
        // reset to start at the next column chunk
        start = -1;
        length = 0;
      }

      currentBlock.addColumn(ColumnChunkMetaData.get(
          chunk.getPath(),
          chunk.getType(),
          chunk.getCodec(),
          chunk.getEncodingStats(),
          chunk.getEncodings(),
          chunk.getStatistics(),
          newChunkStart,
          newChunkStart,
          chunk.getValueCount(),
          chunk.getTotalSize(),
          chunk.getTotalUncompressedSize()));

      blockCompressedSize += chunk.getTotalSize();
    }

    currentBlock.setTotalByteSize(blockCompressedSize);

    endBlock();
  }

  // Buffers for the copy function.
  private static final ThreadLocal<byte[]> COPY_BUFFER =
      new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
          return new byte[8192];
        }
      };

  /**
   * Copy from a FS input stream to an output stream. Thread-safe
   *
   * @param from a {@link FSDataInputStream}
   * @param to any {@link OutputStream}
   * @param start where in the from stream to start copying
   * @param length the number of bytes to copy
   * @throws IOException
   */
  private static void copy(SeekableInputStream from, FSDataOutputStream to,
                           long start, long length) throws IOException{
    LOG.debug("Copying {} bytes at {} to {}" ,length , start , to.getPos());
    from.seek(start);
    long bytesCopied = 0;
    byte[] buffer = COPY_BUFFER.get();
    while (bytesCopied < length) {
      long bytesLeft = length - bytesCopied;
      int bytesRead = from.read(buffer, 0,
          (buffer.length < bytesLeft ? buffer.length : (int) bytesLeft));
      if (bytesRead < 0) {
        throw new IllegalArgumentException(
            "Unexpected end of input file at " + start + bytesCopied);
      }
      to.write(buffer, 0, bytesRead);
      bytesCopied += bytesRead;
    }
  }

  /**
   * ends a file once all blocks have been written.
   * closes the file.
   * @param extraMetaData the extra meta data to write in the footer
   * @throws IOException
   */
  public void end(Map<String, String> extraMetaData) throws IOException {
    state = state.end();
    LOG.debug("{}: end", out.getPos());
    ParquetMetadata footer = new ParquetMetadata(new FileMetaData(schema, extraMetaData, Version.FULL_VERSION), blocks);
    serializeFooter(footer, out);
    out.close();
  }

  private static void serializeFooter(ParquetMetadata footer, FSDataOutputStream out) throws IOException {
    long footerIndex = out.getPos();
    org.apache.parquet.format.FileMetaData parquetMetadata = metadataConverter.toParquetMetadata(CURRENT_VERSION, footer);
    writeFileMetaData(parquetMetadata, out);
    LOG.debug("{}: footer length = {}" , out.getPos(), (out.getPos() - footerIndex));
    BytesUtils.writeIntLittleEndian(out, (int)(out.getPos() - footerIndex));
    out.write(MAGIC);
  }

  /**
   * writes a _metadata and _common_metadata file
   * @param configuration the configuration to use to get the FileSystem
   * @param outputPath the directory to write the _metadata file to
   * @param footers the list of footers to merge
   * @throws IOException
   */
  public static void writeMetadataFile(Configuration configuration, Path outputPath, List<Footer> footers) throws IOException {
    FileSystem fs = outputPath.getFileSystem(configuration);
    outputPath = outputPath.makeQualified(fs);
    ParquetMetadata metadataFooter = mergeFooters(outputPath, footers);
    writeMetadataFile(outputPath, metadataFooter, fs, PARQUET_METADATA_FILE);
    metadataFooter.getBlocks().clear();
    writeMetadataFile(outputPath, metadataFooter, fs, PARQUET_COMMON_METADATA_FILE);
  }

  private static void writeMetadataFile(Path outputPath, ParquetMetadata metadataFooter, FileSystem fs, String parquetMetadataFile)
      throws IOException {
    Path metaDataPath = new Path(outputPath, parquetMetadataFile);
    FSDataOutputStream metadata = fs.create(metaDataPath);
    metadata.write(MAGIC);
    serializeFooter(metadataFooter, metadata);
    metadata.close();
  }

  static ParquetMetadata mergeFooters(Path root, List<Footer> footers) {
    String rootPath = root.toUri().getPath();
    GlobalMetaData fileMetaData = null;
    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
    for (Footer footer : footers) {
        String footerPath = footer.getFile().toUri().getPath();
      if (!footerPath.startsWith(rootPath)) {
        throw new ParquetEncodingException(footerPath + " invalid: all the files must be contained in the root " + root);
      }
      footerPath = footerPath.substring(rootPath.length());
      while (footerPath.startsWith("/")) {
        footerPath = footerPath.substring(1);
      }
      fileMetaData = mergeInto(footer.getParquetMetadata().getFileMetaData(), fileMetaData);
      for (BlockMetaData block : footer.getParquetMetadata().getBlocks()) {
        block.setPath(footerPath);
        blocks.add(block);
      }
    }
    return new ParquetMetadata(fileMetaData.merge(), blocks);
  }

  /**
   * @return the current position in the underlying file
   * @throws IOException
   */
  public long getPos() throws IOException {
    return out.getPos();
  }

  public long getNextRowGroupSize() throws IOException {
    return alignment.nextRowGroupSize(out);
  }

  /**
   * Will merge the metadata of all the footers together
   * @param footers the list files footers to merge
   * @return the global meta data for all the footers
   */
  static GlobalMetaData getGlobalMetaData(List<Footer> footers) {
    return getGlobalMetaData(footers, true);
  }

  static GlobalMetaData getGlobalMetaData(List<Footer> footers, boolean strict) {
    GlobalMetaData fileMetaData = null;
    for (Footer footer : footers) {
      ParquetMetadata currentMetadata = footer.getParquetMetadata();
      fileMetaData = mergeInto(currentMetadata.getFileMetaData(), fileMetaData, strict);
    }
    return fileMetaData;
  }

  /**
   * Will return the result of merging toMerge into mergedMetadata
   * @param toMerge the metadata toMerge
   * @param mergedMetadata the reference metadata to merge into
   * @return the result of the merge
   */
  static GlobalMetaData mergeInto(
      FileMetaData toMerge,
      GlobalMetaData mergedMetadata) {
    return mergeInto(toMerge, mergedMetadata, true);
  }

  static GlobalMetaData mergeInto(
      FileMetaData toMerge,
      GlobalMetaData mergedMetadata,
      boolean strict) {
    MessageType schema = null;
    Map<String, Set<String>> newKeyValues = new HashMap<String, Set<String>>();
    Set<String> createdBy = new HashSet<String>();
    if (mergedMetadata != null) {
      schema = mergedMetadata.getSchema();
      newKeyValues.putAll(mergedMetadata.getKeyValueMetaData());
      createdBy.addAll(mergedMetadata.getCreatedBy());
    }
    if ((schema == null && toMerge.getSchema() != null)
        || (schema != null && !schema.equals(toMerge.getSchema()))) {
      schema = mergeInto(toMerge.getSchema(), schema, strict);
    }
    for (Entry<String, String> entry : toMerge.getKeyValueMetaData().entrySet()) {
      Set<String> values = newKeyValues.get(entry.getKey());
      if (values == null) {
        values = new LinkedHashSet<String>();
        newKeyValues.put(entry.getKey(), values);
      }
      values.add(entry.getValue());
    }
    createdBy.add(toMerge.getCreatedBy());
    return new GlobalMetaData(
        schema,
        newKeyValues,
        createdBy);
  }

  /**
   * will return the result of merging toMerge into mergedSchema
   * @param toMerge the schema to merge into mergedSchema
   * @param mergedSchema the schema to append the fields to
   * @return the resulting schema
   */
  static MessageType mergeInto(MessageType toMerge, MessageType mergedSchema) {
    return mergeInto(toMerge, mergedSchema, true);
  }

  /**
   * will return the result of merging toMerge into mergedSchema
   * @param toMerge the schema to merge into mergedSchema
   * @param mergedSchema the schema to append the fields to
   * @param strict should schema primitive types match
   * @return the resulting schema
   */
  static MessageType mergeInto(MessageType toMerge, MessageType mergedSchema, boolean strict) {
    if (mergedSchema == null) {
      return toMerge;
    }

    return mergedSchema.union(toMerge, strict);
  }

  private interface AlignmentStrategy {
    void alignForRowGroup(FSDataOutputStream out) throws IOException;

    long nextRowGroupSize(FSDataOutputStream out) throws IOException;
  }

  private static class NoAlignment implements AlignmentStrategy {
    public static NoAlignment get(long rowGroupSize) {
      return new NoAlignment(rowGroupSize);
    }

    private final long rowGroupSize;

    private NoAlignment(long rowGroupSize) {
      this.rowGroupSize = rowGroupSize;
    }

    @Override
    public void alignForRowGroup(FSDataOutputStream out) {
    }

    @Override
    public long nextRowGroupSize(FSDataOutputStream out) {
      return rowGroupSize;
    }
  }

  /**
   * Alignment strategy that pads when less than half the row group size is
   * left before the next DFS block.
   */
  private static class PaddingAlignment implements AlignmentStrategy {
    private static final byte[] zeros = new byte[4096];

    public static PaddingAlignment get(long dfsBlockSize, long rowGroupSize,
                                       int maxPaddingSize) {
      return new PaddingAlignment(dfsBlockSize, rowGroupSize, maxPaddingSize);
    }

    protected final long dfsBlockSize;
    protected final long rowGroupSize;
    protected final int maxPaddingSize;

    private PaddingAlignment(long dfsBlockSize, long rowGroupSize,
                             int maxPaddingSize) {
      this.dfsBlockSize = dfsBlockSize;
      this.rowGroupSize = rowGroupSize;
      this.maxPaddingSize = maxPaddingSize;
    }

    @Override
    public void alignForRowGroup(FSDataOutputStream out) throws IOException {
      long remaining = dfsBlockSize - (out.getPos() % dfsBlockSize);

      if (isPaddingNeeded(remaining)) {
        LOG.debug("Adding {} bytes of padding (row group size={}B, block size={}B)", remaining, rowGroupSize, dfsBlockSize);
        for (; remaining > 0; remaining -= zeros.length) {
          out.write(zeros, 0, (int) Math.min((long) zeros.length, remaining));
        }
      }
    }

    @Override
    public long nextRowGroupSize(FSDataOutputStream out) throws IOException {
      if (maxPaddingSize <= 0) {
        return rowGroupSize;
      }

      long remaining = dfsBlockSize - (out.getPos() % dfsBlockSize);

      if (isPaddingNeeded(remaining)) {
        return rowGroupSize;
      }

      return Math.min(remaining, rowGroupSize);
    }

    protected boolean isPaddingNeeded(long remaining) {
      return (remaining <= maxPaddingSize);
    }
  }
}
