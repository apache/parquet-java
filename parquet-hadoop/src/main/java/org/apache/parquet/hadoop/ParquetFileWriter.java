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

import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.format.Util.writeFileMetaData;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.Log;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
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
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * Internal implementation of the Parquet file writer as a block container
 *
 * @author Julien Le Dem
 *
 */
public class ParquetFileWriter {
  private static final Log LOG = Log.getLog(ParquetFileWriter.class);

  public static final String PARQUET_METADATA_FILE = "_metadata";
  public static final String PARQUET_COMMON_METADATA_FILE = "_common_metadata";
  public static final byte[] MAGIC = "PAR1".getBytes(Charset.forName("ASCII"));
  public static final int CURRENT_VERSION = 1;

  // File creation modes
  public static enum Mode {
    CREATE,
    OVERWRITE
  }

  private static final ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();

  private final MessageType schema;
  private final FSDataOutputStream out;
  private BlockMetaData currentBlock;
  private ColumnChunkMetaData currentColumn;
  private long currentRecordCount;
  private List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
  private long uncompressedLength;
  private long compressedLength;
  private Set<org.apache.parquet.column.Encoding> currentEncodings;

  private CompressionCodecName currentChunkCodec;
  private ColumnPath currentChunkPath;
  private PrimitiveTypeName currentChunkType;
  private long currentChunkFirstDataPage;
  private long currentChunkDictionaryPageOffset;
  private long currentChunkValueCount;

  private Statistics currentStatistics;

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
    this(configuration, schema, file, Mode.CREATE);
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
    super();
    this.schema = schema;
    FileSystem fs = file.getFileSystem(configuration);
    boolean overwriteFlag = (mode == Mode.OVERWRITE);
    this.out = fs.create(file, overwriteFlag);
  }

  /**
   * start the file
   * @throws IOException
   */
  public void start() throws IOException {
    state = state.start();
    if (DEBUG) LOG.debug(out.getPos() + ": start");
    out.write(MAGIC);
  }

  /**
   * start a block
   * @param recordCount the record count in this block
   * @throws IOException
   */
  public void startBlock(long recordCount) throws IOException {
    state = state.startBlock();
    if (DEBUG) LOG.debug(out.getPos() + ": start block");
//    out.write(MAGIC); // TODO: add a magic delimiter
    currentBlock = new BlockMetaData();
    currentRecordCount = recordCount;
  }

  /**
   * start a column inside a block
   * @param descriptor the column descriptor
   * @param valueCount the value count in this column
   * @param statistics the statistics in this column
   * @param compressionCodecName
   * @throws IOException
   */
  public void startColumn(ColumnDescriptor descriptor,
                          long valueCount,
                          CompressionCodecName compressionCodecName) throws IOException {
    state = state.startColumn();
    if (DEBUG) LOG.debug(out.getPos() + ": start column: " + descriptor + " count=" + valueCount);
    currentEncodings = new HashSet<org.apache.parquet.column.Encoding>();
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
    if (DEBUG) LOG.debug(out.getPos() + ": write dictionary page: " + dictionaryPage.getDictionarySize() + " values");
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
    if (DEBUG) LOG.debug(out.getPos() + ": write dictionary page content " + compressedPageSize);
    dictionaryPage.getBytes().writeAllTo(out);
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
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding) throws IOException {
    state = state.write();
    long beforeHeader = out.getPos();
    if (DEBUG) LOG.debug(beforeHeader + ": write data page: " + valueCount + " values");
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
    if (DEBUG) LOG.debug(out.getPos() + ": write data page content " + compressedPageSize);
    bytes.writeAllTo(out);
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
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding) throws IOException {
    state = state.write();
    long beforeHeader = out.getPos();
    if (DEBUG) LOG.debug(beforeHeader + ": write data page: " + valueCount + " values");
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
    if (DEBUG) LOG.debug(out.getPos() + ": write data page content " + compressedPageSize);
    bytes.writeAllTo(out);
    currentStatistics.mergeStatistics(statistics);
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
                       List<org.apache.parquet.column.Encoding> encodings) throws IOException {
    state = state.write();
    if (DEBUG) LOG.debug(out.getPos() + ": write data pages");
    long headersSize = bytes.size() - compressedTotalPageSize;
    this.uncompressedLength += uncompressedTotalPageSize + headersSize;
    this.compressedLength += compressedTotalPageSize + headersSize;
    if (DEBUG) LOG.debug(out.getPos() + ": write data pages content");
    bytes.writeAllTo(out);
    currentEncodings.addAll(encodings);
    currentStatistics = totalStats;
  }

  /**
   * end a column (once all rep, def and data have been written)
   * @throws IOException
   */
  public void endColumn() throws IOException {
    state = state.endColumn();
    if (DEBUG) LOG.debug(out.getPos() + ": end column");
    currentBlock.addColumn(ColumnChunkMetaData.get(
        currentChunkPath,
        currentChunkType,
        currentChunkCodec,
        currentEncodings,
        currentStatistics,
        currentChunkFirstDataPage,
        currentChunkDictionaryPageOffset,
        currentChunkValueCount,
        compressedLength,
        uncompressedLength));
    if (DEBUG) LOG.info("ended Column chumk: " + currentColumn);
    currentColumn = null;
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
    if (DEBUG) LOG.debug(out.getPos() + ": end block");
    currentBlock.setRowCount(currentRecordCount);
    blocks.add(currentBlock);
    currentBlock = null;
  }

  /**
   * ends a file once all blocks have been written.
   * closes the file.
   * @param extraMetaData the extra meta data to write in the footer
   * @throws IOException
   */
  public void end(Map<String, String> extraMetaData) throws IOException {
    state = state.end();
    if (DEBUG) LOG.debug(out.getPos() + ": end");
    ParquetMetadata footer = new ParquetMetadata(new FileMetaData(schema, extraMetaData, Version.FULL_VERSION), blocks);
    serializeFooter(footer, out);
    out.close();
  }

  private static void serializeFooter(ParquetMetadata footer, FSDataOutputStream out) throws IOException {
    long footerIndex = out.getPos();
    org.apache.parquet.format.FileMetaData parquetMetadata = new ParquetMetadataConverter().toParquetMetadata(CURRENT_VERSION, footer);
    writeFileMetaData(parquetMetadata, out);
    if (DEBUG) LOG.debug(out.getPos() + ": footer length = " + (out.getPos() - footerIndex));
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
    ParquetMetadata metadataFooter = mergeFooters(outputPath, footers);
    FileSystem fs = outputPath.getFileSystem(configuration);
    outputPath = outputPath.makeQualified(fs);
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
        values = new HashSet<String>();
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

}
