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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.InternalColumnEncryptionSetup;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.parquet.column.ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH;
import static org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;

public class ColumnEncryptor {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnEncryptor.class);

  private static class EncryptorRunTime {
    private final InternalColumnEncryptionSetup colEncrSetup;
    private final BlockCipher.Encryptor dataEncryptor;
    private final BlockCipher.Encryptor metaDataEncryptor;
    private final byte[] fileAAD ;

    private byte[] dataPageHeaderAAD;
    private byte[] dataPageAAD;
    private byte[] dictPageHeaderAAD;
    private byte[] dictPageAAD;

    public EncryptorRunTime(InternalFileEncryptor fileEncryptor, ColumnChunkMetaData chunk,
                            int blockId, int columnId) throws IOException  {
      this.colEncrSetup = fileEncryptor.getColumnSetup(chunk.getPath(), true, toShortWithCheck(columnId));
      this.dataEncryptor = colEncrSetup.getDataEncryptor();
      this.metaDataEncryptor = colEncrSetup.getMetaDataEncryptor();

      this.fileAAD = fileEncryptor.getFileAAD();
      this.dataPageHeaderAAD = AesCipher.createModuleAAD(fileAAD, ModuleType.DataPageHeader,
        toShortWithCheck(blockId), toShortWithCheck(columnId), (short) 0);
      this.dataPageAAD = AesCipher.createModuleAAD(fileAAD, ModuleType.DataPage,
        toShortWithCheck(blockId), toShortWithCheck(columnId), (short) 0);
      this.dictPageHeaderAAD = AesCipher.createModuleAAD(fileAAD, ModuleType.DictionaryPageHeader,
        toShortWithCheck(blockId), toShortWithCheck(columnId), (short) 0);
      this.dictPageAAD = AesCipher.createModuleAAD(fileAAD, ModuleType.DictionaryPage,
        toShortWithCheck(blockId), toShortWithCheck(columnId), (short) 0);
    }

    public InternalColumnEncryptionSetup getColEncrSetup() {
      return this.colEncrSetup;
    }

    public BlockCipher.Encryptor getDataEncryptor() {
      return this.dataEncryptor;
    }

    public BlockCipher.Encryptor getMetaDataEncryptor() {
      return this.metaDataEncryptor;
    }

    public byte[] getFileAAD() {
      return this.fileAAD;
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

  private final int pageBufferSize = ParquetProperties.DEFAULT_PAGE_SIZE * 2;
  private byte[] pageBuffer;
  private Configuration conf;

  public ColumnEncryptor(Configuration conf) {
    this.pageBuffer = new byte[pageBufferSize];
    this.conf = conf;
  }

  /**
   *
   * @param inputFile Input file
   * @param outputFile Output file
   * @throws IOException
   */
  public void encryptColumns(String inputFile, String outputFile, List<String> paths, FileEncryptionProperties fileEncryptionProperties) throws IOException {
    Path inPath = new Path(inputFile);
    Path outPath = new Path(outputFile);

    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inPath, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();

    ParquetFileWriter writer = new ParquetFileWriter(HadoopOutputFile.fromPath(outPath, conf), schema, ParquetFileWriter.Mode.OVERWRITE,
      DEFAULT_BLOCK_SIZE, MAX_PADDING_SIZE_DEFAULT, DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH, DEFAULT_STATISTICS_TRUNCATE_LENGTH,
      ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED, fileEncryptionProperties);
    writer.start();

    try (TransParquetFileReader reader = new TransParquetFileReader(HadoopInputFile.fromPath(inPath, conf), HadoopReadOptions.builder(conf).build())) {
      processBlocks(reader, writer, metaData, schema, paths);
    } catch (Exception e) {
      LOG.error("Exception happened while processing blocks: ", e);
    } finally {
      try {
        writer.end(metaData.getFileMetaData().getKeyValueMetaData());
      } catch (Exception e) {
        LOG.error("Exception happened when ending the writer: ", e);
      }
    }
  }

  public void processBlocks(TransParquetFileReader reader, ParquetFileWriter writer, ParquetMetadata meta,
                            MessageType schema, List<String> paths) throws IOException {
    Set<ColumnPath> nullifyColumns = convertToColumnPaths(paths);
    int blockId = 0;
    PageReadStore store = reader.readNextRowGroup();

    while (store != null) {
      writer.startBlock(store.getRowCount());

      List<ColumnChunkMetaData> columnsInOrder = meta.getBlocks().get(blockId).getColumns();
      Map<ColumnPath, ColumnDescriptor> descriptorsMap = schema.getColumns().stream().collect(
        Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));

      for (int i = 0; i < columnsInOrder.size(); i += 1) {
        ColumnChunkMetaData chunk = columnsInOrder.get(i);
        ColumnDescriptor descriptor = descriptorsMap.get(chunk.getPath());
        processChunk(descriptor, chunk, reader, writer, nullifyColumns, blockId, i, meta.getFileMetaData().getCreatedBy());
      }

      writer.endBlock();
      store = reader.readNextRowGroup();
      blockId++;
    }
  }

  private void processChunk(ColumnDescriptor descriptor, ColumnChunkMetaData chunk, TransParquetFileReader reader, ParquetFileWriter writer,
                            Set<ColumnPath> paths, int blockId, int columnId, String createdBy) throws IOException {
    reader.setStreamPosition(chunk.getStartingPos());
    if (paths.contains(chunk.getPath())) {
      writer.startColumn(descriptor, chunk.getValueCount(), chunk.getCodec());
      encryptPages(reader, chunk, writer, createdBy, blockId, columnId);
      writer.endColumn();
    } else {
      BloomFilter bloomFilter = reader.readBloomFilter(chunk);
      ColumnIndex columnIndex = reader.readColumnIndex(chunk);
      OffsetIndex offsetIndex = reader.readOffsetIndex(chunk);
      writer.appendColumnChunk(descriptor, reader.getStream(), chunk, bloomFilter, columnIndex, offsetIndex);
    }
  }

  private void encryptPages(TransParquetFileReader reader, ColumnChunkMetaData chunk,
                             ParquetFileWriter writer, String createdBy, int blockId, int columnId) throws IOException {
    short pageOrdinal = 0;
    EncryptorRunTime encryptorRunTime = new EncryptorRunTime(writer.getEncryptor(), chunk, blockId, columnId);
    DictionaryPage dictionaryPage = null;
    long readValues = 0;
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    long totalChunkValues = chunk.getValueCount();
    while (readValues < totalChunkValues) {
      if (Short.MAX_VALUE == pageOrdinal) {
        throw new RuntimeException("Number of pages exceeds maximum: " + Short.MAX_VALUE);
      }
      PageHeader pageHeader = reader.readPageHeader();
      byte[] pageLoad;
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          if (dictionaryPage != null) {
            throw new IOException("has more than one dictionary page in column chunk");
          }
          //No quickUpdatePageAAD needed for dictionary page
          DictionaryPageHeader dictPageHeader = pageHeader.dictionary_page_header;
          pageLoad = encryptPageLoad(reader, pageHeader.getCompressed_page_size(), encryptorRunTime.getDataEncryptor(), encryptorRunTime.getDictPageAAD());
          writer.writeDictionaryPage(new DictionaryPage(BytesInput.from(pageLoad),
                                        pageHeader.getUncompressed_page_size(),
                                        dictPageHeader.getNum_values(),
                                        converter.getEncoding(dictPageHeader.getEncoding())),
            encryptorRunTime.getMetaDataEncryptor(), encryptorRunTime.getDictPageHeaderAAD());
          break;
        case DATA_PAGE:
          AesCipher.quickUpdatePageAAD(encryptorRunTime.getDataPageHeaderAAD(), pageOrdinal);
          AesCipher.quickUpdatePageAAD(encryptorRunTime.getDataPageAAD(), pageOrdinal);
          DataPageHeader headerV1 = pageHeader.data_page_header;
          pageLoad = encryptPageLoad(reader, pageHeader.getCompressed_page_size(), encryptorRunTime.getDataEncryptor(), encryptorRunTime.getDataPageAAD());
          readValues += headerV1.getNum_values();
          writer.writeDataPage(toIntWithCheck(headerV1.getNum_values()),
              pageHeader.getUncompressed_page_size(),
              BytesInput.from(pageLoad),
              converter.fromParquetStatistics(createdBy, headerV1.getStatistics(), chunk.getPrimitiveType()),
              converter.getEncoding(headerV1.getRepetition_level_encoding()),
              converter.getEncoding(headerV1.getDefinition_level_encoding()),
              converter.getEncoding(headerV1.getEncoding()),
            encryptorRunTime.getMetaDataEncryptor(),
            encryptorRunTime.getDataPageHeaderAAD());
          pageOrdinal++;
          break;
        case DATA_PAGE_V2:
          AesCipher.quickUpdatePageAAD(encryptorRunTime.getDataPageHeaderAAD(), pageOrdinal);
          AesCipher.quickUpdatePageAAD(encryptorRunTime.getDataPageAAD(), pageOrdinal);
          DataPageHeaderV2 headerV2 = pageHeader.data_page_header_v2;
          int rlLength = headerV2.getRepetition_levels_byte_length();
          BytesInput rlLevels = readBlockAllocate(rlLength, reader);
          int dlLength = headerV2.getDefinition_levels_byte_length();
          BytesInput dlLevels = readBlockAllocate(dlLength, reader);
          int payLoadLength = pageHeader.getCompressed_page_size() - rlLength - dlLength;
          int rawDataLength = pageHeader.getUncompressed_page_size() - rlLength - dlLength;
          pageLoad = encryptPageLoad(reader, payLoadLength, encryptorRunTime.getDataEncryptor(), encryptorRunTime.getDataPageAAD());
          readValues += headerV2.getNum_values();
          writer.writeDataPageV2(headerV2.getNum_rows(),
            headerV2.getNum_nulls(),
            headerV2.getNum_values(),
            rlLevels,
            dlLevels,
            converter.getEncoding(headerV2.getEncoding()),
            BytesInput.from(pageLoad),
            rawDataLength,
            converter.fromParquetStatistics(createdBy, headerV2.getStatistics(), chunk.getPrimitiveType()));
          pageOrdinal++;
          break;
        default:
        break;
      }
    }
  }

  private byte[] encryptPageLoad(TransParquetFileReader reader, int payloadLength, BlockCipher.Encryptor dataEncryptor, byte[] AAD) throws IOException {
    byte[] data = readBlock(payloadLength, reader);
    return dataEncryptor.encrypt(data, AAD);
  }

  public byte[] readBlock(int length, TransParquetFileReader reader) throws IOException {
    byte[] data;
    if (length > pageBufferSize) {
      data = new byte[length];
    } else {
      data = pageBuffer;
    }
    reader.blockRead(data, 0, length);
    return data;
  }

  public BytesInput readBlockAllocate(int length, TransParquetFileReader reader) throws IOException {
    byte[] data = new byte[length];
    reader.blockRead(data, 0, length);
    return BytesInput.from(data, 0, length);
  }

  private int toIntWithCheck(long size) {
    if ((int)size != size) {
      throw new ParquetEncodingException("size is bigger than " + Integer.MAX_VALUE + " bytes: " + size);
    }
    return (int)size;
  }

  private static short toShortWithCheck(int size) {
    if ((short) size != size) {
      throw new ParquetEncodingException("size " + size + " is bigger than " + Short.MAX_VALUE);
    }
    return (short) size;
  }

  public static Set<ColumnPath> convertToColumnPaths(List<String> cols) {
    Set<ColumnPath> prunePaths = new HashSet<>();
    for (String col : cols) {
      prunePaths.add(ColumnPath.fromDotString(col));
    }
    return prunePaths;
  }

  public enum EncryptMode {
    AES_GCM_CTR("AES_GCM_CTR_V1"),
    AES_GCM("AES_GCM_V1");

    private String mode;

    EncryptMode(String text) {
      this.mode = text;
    }

    public String getMode() {
      return this.mode;
    }

    public static EncryptMode fromString(String mode) {
      for (EncryptMode b : EncryptMode.values()) {
        if (b.mode.equalsIgnoreCase(mode)) {
          return b;
        }
      }
      return null;
    }
  }
}
