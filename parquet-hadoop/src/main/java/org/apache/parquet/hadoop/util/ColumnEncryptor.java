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
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalColumnEncryptionSetup;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

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

/**
 * This class is for fast rewriting existing file with column encryption
 *
 * For columns to be encrypted, all the pages of those columns are read, but decompression/decoding,
 * it is encrypted immediately and write back.
 *
 * For columns not to be encrypted, the whole column chunk will be appended directly to writer.
 */
public class ColumnEncryptor {
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
      if (fileEncryptor == null) {
        this.colEncrSetup = null;
        this.dataEncryptor =  null;
        this.metaDataEncryptor =  null;

        this.fileAAD =  null;
        this.dataPageHeaderAAD =  null;
        this.dataPageAAD =  null;
        this.dictPageHeaderAAD =  null;
        this.dictPageAAD =  null;
      } else {
        this.colEncrSetup = fileEncryptor.getColumnSetup(chunk.getPath(), true, columnId);
        this.dataEncryptor = colEncrSetup.getDataEncryptor();
        this.metaDataEncryptor = colEncrSetup.getMetaDataEncryptor();

        this.fileAAD = fileEncryptor.getFileAAD();
        this.dataPageHeaderAAD = createAAD(colEncrSetup, ModuleType.DataPageHeader, blockId, columnId);
        this.dataPageAAD = createAAD(colEncrSetup, ModuleType.DataPage, blockId, columnId);
        this.dictPageHeaderAAD = createAAD(colEncrSetup, ModuleType.DictionaryPageHeader, blockId, columnId);
        this.dictPageAAD = createAAD(colEncrSetup, ModuleType.DictionaryPage, blockId, columnId);
      }
    }

    private byte[] createAAD(InternalColumnEncryptionSetup colEncrSetup, ModuleType moduleType, int blockId, int columnId) {
      if (colEncrSetup != null && colEncrSetup.isEncrypted()) {
        return AesCipher.createModuleAAD(fileAAD, moduleType, blockId, columnId, 0);
      }
      return null;
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

  private static class DecryptorRunTime {
    private final BlockCipher.Decryptor headerBlockDecryptor;
    private final BlockCipher.Decryptor dataDecryptor;
    private final byte[] fileAAD;
    private final Short columnOrdinal;

    public DecryptorRunTime(InternalFileDecryptor fileDecryptor, ColumnChunkMetaData chunk) throws IOException  {
      if (fileDecryptor == null) {
        this.headerBlockDecryptor = null;
        this.dataDecryptor = null;
        this.fileAAD = null;
        this.columnOrdinal = null;
      } else {
        InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.getColumnSetup(chunk.getPath());
        this.headerBlockDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        this.dataDecryptor = columnDecryptionSetup.getDataDecryptor();
        this.fileAAD = fileDecryptor.getFileAAD();
        this.columnOrdinal = columnDecryptionSetup.getOrdinal();
      }
    }

    public byte[] getFileAAD() {
      return this.fileAAD;
    }

    public short getColumnOrdinal() {
      return this.columnOrdinal.shortValue();
    }

    public BlockCipher.Decryptor getHeaderBlockDecryptor() {
      return this.headerBlockDecryptor;
    }

    public BlockCipher.Decryptor getDataDecryptor() {
      return this.dataDecryptor;
    }
  }

  private Configuration conf;

  public ColumnEncryptor(Configuration conf) {
    this.conf = conf;
  }

  public static ParquetMetadata readFooter(String inputFile, ParquetMetadataConverter.MetadataFilter filter,
                                           FileDecryptionProperties decryptionProperties,
                                           Configuration config) throws IOException {
    ParquetMetadata metaData;
    ParquetReadOptions readOptions = ParquetReadOptions.builder()
      .withDecryption(decryptionProperties)
      .build();
    InputFile file = HadoopInputFile.fromPath(new Path(inputFile), config);
    try (SeekableInputStream in = file.newStream()) {
      metaData  = ParquetFileReader.readFooter(file, readOptions, in);
    }
    return metaData;
  }

  public static Set<ColumnPath> encryptedColumnPaths(String inputFilePath, byte[] footer_key, Configuration config) throws IOException {
    // use plain decrypt props to determine what's column is encrypted.
    // pass footer_key if footer is encrypted
    FileDecryptionProperties plainDecryptProps = FileDecryptionProperties.builder()
      .withFooterKey(footer_key)
      .withPlaintextFilesAllowed()
      .withoutFooterSignatureVerification()
      .withKeyRetriever(null)
      .build();
    Set<ColumnPath> encrColumnPaths = new HashSet<>();

    ParquetMetadata parquetMetadata =  readFooter(inputFilePath, NO_FILTER, plainDecryptProps, config);

    for (BlockMetaData block : parquetMetadata.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        if (column.isEncrypted()) {
          encrColumnPaths.add(column.getPath());
        }
      }
    }

    return encrColumnPaths;
  }

  /**
   * Given the input file, encrypt the columns specified by paths, and output the file.
   * The encryption settings can be specified in the parameter of fileEncryptionProperties
   * @param inputFile Input file
   * @param outputFile Output file
   * @param paths columns to be encrypted
   * @param fileEncryptionProperties FileEncryptionProperties of the file
   * @throws IOException
   */
  public void encryptColumns(String inputFile, String outputFile, List<String> paths, FileEncryptionProperties fileEncryptionProperties,
                             FileDecryptionProperties fileDecryptionProperties) throws IOException {
    Path inPath = new Path(inputFile);
    Path outPath = new Path(outputFile);

    ParquetMetadata metaData = readFooter(inputFile, NO_FILTER, fileDecryptionProperties, conf);
    MessageType schema = metaData.getFileMetaData().getSchema();

    Set<ColumnPath> alreadyEncrColumnPaths = encryptedColumnPaths(inputFile, fileDecryptionProperties.getFooterKey(), conf);

    FileDecryptionProperties fdp = fileDecryptionProperties.deepClone(null);
    InternalFileDecryptor internalFileDecryptor = getFileDecryptorOrNull(inPath, fdp, conf);

    ParquetFileWriter writer = new ParquetFileWriter(HadoopOutputFile.fromPath(outPath, conf), schema, ParquetFileWriter.Mode.OVERWRITE,
      DEFAULT_BLOCK_SIZE, MAX_PADDING_SIZE_DEFAULT, DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH, DEFAULT_STATISTICS_TRUNCATE_LENGTH,
      ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED, fileEncryptionProperties);
    writer.start();

    try (TransParquetFileReader reader = new TransParquetFileReader(HadoopInputFile.fromPath(inPath, conf)
      , HadoopReadOptions.builder(conf).withDecryption(fdp).build())) {
      processBlocks(reader, writer, metaData, schema, paths, alreadyEncrColumnPaths, internalFileDecryptor);
    }
    writer.end(metaData.getFileMetaData().getKeyValueMetaData());
  }

  private void processBlocks(TransParquetFileReader reader, ParquetFileWriter writer, ParquetMetadata meta,
                             MessageType schema, List<String> encryptPaths, Set<ColumnPath> alreadyEncrColumnPaths,
                             InternalFileDecryptor internalFileDecryptor) throws IOException {
    Set<ColumnPath> encryptColumnsPath = convertToColumnPaths(encryptPaths);
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
        processChunk(descriptor, chunk, reader, writer, encryptColumnsPath, alreadyEncrColumnPaths,
          blockId, i, meta.getFileMetaData().getCreatedBy(),
          internalFileDecryptor);
      }

      writer.endBlock();
      store = reader.readNextRowGroup();
      blockId++;
    }
  }

  private void processChunk(ColumnDescriptor descriptor, ColumnChunkMetaData chunk, TransParquetFileReader reader, ParquetFileWriter writer,
                            Set<ColumnPath> needEncrPaths, Set<ColumnPath> alreadyEncrColumnPaths, int blockId, int columnId, String createdBy,
                            InternalFileDecryptor internalFileDecryptor) throws IOException {
    reader.setStreamPosition(chunk.getStartingPos());

    EncryptorRunTime encryptorRunTime = new EncryptorRunTime(writer.getEncryptor(), chunk, blockId, columnId);
    writer.startColumn(descriptor, chunk.getValueCount(), chunk.getCodec());

    Boolean alreadyEncrypted = alreadyEncrColumnPaths.size() > 0;
    DecryptorRunTime decryptorRunTime = alreadyEncrypted? new DecryptorRunTime(internalFileDecryptor, chunk): null;
    processPages(reader, chunk, encryptorRunTime, decryptorRunTime, writer, createdBy,
      needEncrPaths.contains(chunk.getPath()), alreadyEncrColumnPaths.contains(chunk.getPath()), alreadyEncrypted);

    writer.endColumn();
  }

  private void processPages(TransParquetFileReader reader, ColumnChunkMetaData chunk,
                            EncryptorRunTime encryptorRunTime, DecryptorRunTime decryptorRunTime,
                            ParquetFileWriter writer, String createdBy,
                            Boolean needEncryptAgain, Boolean needDecrypt, Boolean alreadyEncrypted) throws IOException {
    short pageOrdinal = 0;
    DictionaryPage dictionaryPage = null;
    long readValues = 0;
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    long totalChunkValues = chunk.getValueCount();

    BlockCipher.Decryptor headerBlockDecryptor = null;
    BlockCipher.Decryptor dataDecryptor = null;
    byte[] aadPrefix = null;
    byte[] dataPageHeaderAAD = null;

    if (alreadyEncrypted) {
      headerBlockDecryptor = decryptorRunTime.getHeaderBlockDecryptor();
      aadPrefix = decryptorRunTime.getFileAAD();
      if (null != headerBlockDecryptor) {
        dataPageHeaderAAD = AesCipher.createModuleAAD(aadPrefix, ModuleType.DataPageHeader, chunk.getRowGroupOrdinal(), decryptorRunTime.getColumnOrdinal(), pageOrdinal);
      }

      dataDecryptor = decryptorRunTime.getDataDecryptor();
    }

    while (readValues < totalChunkValues) {
      if (Short.MAX_VALUE == pageOrdinal) {
        throw new RuntimeException("Number of pages exceeds maximum: " + Short.MAX_VALUE);
      }

      byte[] pageHeaderAAD = null;
      if (alreadyEncrypted) {
        pageHeaderAAD = dataPageHeaderAAD;
        if (null != headerBlockDecryptor) {
          // This presumes dictionary page (if present) is the first page in chunk
          if (null == dictionaryPage && chunk.hasDictionaryPage()) {
            pageHeaderAAD = AesCipher.createModuleAAD(aadPrefix, ModuleType.DictionaryPageHeader, chunk.getRowGroupOrdinal(), decryptorRunTime.getColumnOrdinal(), (short) -1);
          } else {
            if (pageOrdinal < 0) break;
            AesCipher.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
          }
        }
      }

      PageHeader pageHeader = reader.readPageHeader(headerBlockDecryptor, pageHeaderAAD);
      byte[] pageLoad;
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          if (dictionaryPage != null) {
            throw new IOException("has more than one dictionary page in column chunk");
          }
          //No quickUpdatePageAAD needed for dictionary page
          DictionaryPageHeader dictPageHeader = pageHeader.dictionary_page_header;
          pageLoad = encryptPageLoad(reader,
            pageHeader.getCompressed_page_size(),
            needEncryptAgain? encryptorRunTime.getDictPageAAD(): null,
            dataDecryptor,
            needEncryptAgain? encryptorRunTime.getDataEncryptor(): null,
            needEncryptAgain, needDecrypt);
          dictionaryPage = new DictionaryPage(BytesInput.from(pageLoad),
            pageHeader.getUncompressed_page_size(),
            dictPageHeader.getNum_values(),
            converter.getEncoding(dictPageHeader.getEncoding()));
          writer.writeDictionaryPage(dictionaryPage,
            needEncryptAgain? encryptorRunTime.getMetaDataEncryptor(): null,
            needEncryptAgain? encryptorRunTime.getDictPageHeaderAAD(): null);
          break;
        case DATA_PAGE:
          if (needEncryptAgain){
            AesCipher.quickUpdatePageAAD(encryptorRunTime.getDataPageHeaderAAD(), pageOrdinal);
            AesCipher.quickUpdatePageAAD(encryptorRunTime.getDataPageAAD(), pageOrdinal);
          }
          DataPageHeader headerV1 = pageHeader.data_page_header;
          pageLoad = encryptPageLoad(reader,
            pageHeader.getCompressed_page_size(),
            needEncryptAgain? encryptorRunTime.getDataPageAAD(): null,
            dataDecryptor,
            needEncryptAgain? encryptorRunTime.getDataEncryptor(): null,
            needEncryptAgain,
            needDecrypt);
          readValues += headerV1.getNum_values();
          writer.writeDataPage(Math.toIntExact(headerV1.getNum_values()),
            pageHeader.getUncompressed_page_size(),
            BytesInput.from(pageLoad),
            converter.fromParquetStatistics(createdBy, headerV1.getStatistics(), chunk.getPrimitiveType()),
            converter.getEncoding(headerV1.getRepetition_level_encoding()),
            converter.getEncoding(headerV1.getDefinition_level_encoding()),
            converter.getEncoding(headerV1.getEncoding()),
            needEncryptAgain? encryptorRunTime.getMetaDataEncryptor(): null,
            needEncryptAgain? encryptorRunTime.getDataPageHeaderAAD(): null);
          pageOrdinal++;
          break;
        case DATA_PAGE_V2:
          if (needEncryptAgain){
            AesCipher.quickUpdatePageAAD(encryptorRunTime.getDataPageHeaderAAD(), pageOrdinal);
            AesCipher.quickUpdatePageAAD(encryptorRunTime.getDataPageAAD(), pageOrdinal);
          }
          DataPageHeaderV2 headerV2 = pageHeader.data_page_header_v2;
          int rlLength = headerV2.getRepetition_levels_byte_length();
          BytesInput rlLevels = readBlockAllocate(rlLength, reader);
          int dlLength = headerV2.getDefinition_levels_byte_length();
          BytesInput dlLevels = readBlockAllocate(dlLength, reader);
          int payLoadLength = pageHeader.getCompressed_page_size() - rlLength - dlLength;
          int rawDataLength = pageHeader.getUncompressed_page_size() - rlLength - dlLength;
          pageLoad = encryptPageLoad(reader,
            payLoadLength,
            needEncryptAgain? encryptorRunTime.getDataPageAAD(): null,
            dataDecryptor,
            needEncryptAgain? encryptorRunTime.getDataEncryptor(): null,
            needEncryptAgain,
            needDecrypt);
          readValues += headerV2.getNum_values();
          writer.writeDataPageV2(headerV2.getNum_rows(),
            headerV2.getNum_nulls(),
            headerV2.getNum_values(),
            rlLevels,
            dlLevels,
            converter.getEncoding(headerV2.getEncoding()),
            BytesInput.from(pageLoad),
            rawDataLength,
            converter.fromParquetStatistics(createdBy, headerV2.getStatistics(), chunk.getPrimitiveType()),
            needEncryptAgain? encryptorRunTime.getMetaDataEncryptor(): null,
            needEncryptAgain? encryptorRunTime.getDataPageHeaderAAD(): null);
          pageOrdinal++;
          break;
        default:
          break;
      }
    }
  }

  // encrypt pageload (decrypt pageload before encryption if needed)
  private byte[] encryptPageLoad(TransParquetFileReader reader, int payloadLength, byte[] AAD,
                                 BlockCipher.Decryptor dataDecryptor, BlockCipher.Encryptor dataEncryptor,
                                 Boolean needEncrypt, Boolean needDecrypt) throws IOException {
    // data already encrypted
    byte[] rawData = readBlock(payloadLength, reader);
    byte[] unEncryptedData = needDecrypt && dataDecryptor != null? dataDecryptor.decrypt(rawData, AAD): rawData;

    if (!needEncrypt) {
      return unEncryptedData;
    }

    if (dataEncryptor != null && AAD != null) {
      return dataEncryptor.encrypt(unEncryptedData, AAD);
    } else {
      throw new IOException("Doesn't have non-null dataEncryptor or AAD");
    }
  }

  public byte[] readBlock(int length, TransParquetFileReader reader) throws IOException {
    byte[] data = new byte[length];
    reader.blockRead(data, 0, length);
    return data;
  }

  public BytesInput readBlockAllocate(int length, TransParquetFileReader reader) throws IOException {
    byte[] data = new byte[length];
    reader.blockRead(data, 0, length);
    return BytesInput.from(data, 0, length);
  }

  public static Set<ColumnPath> convertToColumnPaths(List<String> cols) {
    Set<ColumnPath> prunePaths = new HashSet<>();
    for (String col : cols) {
      prunePaths.add(ColumnPath.fromDotString(col));
    }
    return prunePaths;
  }

  public static InternalFileDecryptor getFileDecryptorOrNull(Path filePath, FileDecryptionProperties fileDecryptionProperties,
                                                             Configuration configuration) throws IOException {
    if (fileDecryptionProperties == null) {
      return null;
    }

    InputFile file = HadoopInputFile.fromPath(filePath, configuration);
    ParquetReadOptions options = HadoopReadOptions.builder(configuration).build();
    InternalFileDecryptor internalFileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);

    try (SeekableInputStream in = file.newStream()) {
      // Ignore the return value of readFooter() because we only need to setFileCryptoMetaData() internalFileDecryptor.
      ParquetFileReader.readFooter(file, ParquetReadOptions.builder().build(), in, new ParquetMetadataConverter(),
        fileDecryptionProperties, internalFileDecryptor);
      return internalFileDecryptor;
    }
  }
}
