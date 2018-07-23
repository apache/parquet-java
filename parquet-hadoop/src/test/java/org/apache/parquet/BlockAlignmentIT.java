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
package org.apache.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types.MessageTypeBuilder;
import org.apache.parquet.statistics.RandomValues;
import org.apache.parquet.statistics.TestStatistics;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static junit.framework.Assert.assertEquals;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class contains test cases to validate each data type encoding.
 * Each test runs against all Parquet writer versions.
 * All data types are validating with and without dictionary encoding.
 */
@RunWith(Parameterized.class)
public class BlockAlignmentIT {
  private static final Logger LOG = LoggerFactory.getLogger(BlockAlignmentIT.class);

  private static final boolean KEEP_TEMP_FILES = false;
  private static final int RANDOM_SEED = 1;
  private static final int FIXED_LENGTH = 16;
  private static final int DISTINCT_VALUE_COUNT = 900;

  private static RandomValues.BinaryGenerator binaryGenerator;
  private static RandomValues.FixedGenerator fixedBinaryGenerator;

  // Parameters
  private WriterVersion writerVersion;
  private PrimitiveTypeName paramTypeName;
  private CompressionCodecName compression;
  private int recordCount;
  private int columnCount;
  private int pageSize;
  private int rowGroupSize;
  private int dfsBlockSize;
  private int maxPaddingSize;

  @Parameterized.Parameters(
      // Full set of params
      // name = "{0}; {1}; {2}; recordCount = {3}; columnCount = {4}; pageSize = {5}; rowGroupSize = {6}; dfsBlockSize = {7}; maxPaddingSize = {8}")
      // Relevant params
      name = "{0}; {1}; {2}; rowGroupSize = {6}")
  public static Collection<Object[]> getParameters() {
    List<WriterVersion> writerVersions = Arrays.asList(PARQUET_1_0, PARQUET_2_0);

    List<PrimitiveTypeName> types = Arrays.asList(
        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        PrimitiveTypeName.BINARY);

    List<CompressionCodecName> codecs;
    String codecList = System.getenv("TEST_CODECS");
    if (codecList != null) {
      codecs = new ArrayList<CompressionCodecName>();
      for (String codec : codecList.split(",")) {
        codecs.add(CompressionCodecName.valueOf(codec.toUpperCase(Locale.ENGLISH)));
      }
    } else {
      codecs = Arrays.asList(UNCOMPRESSED, SNAPPY);
    }

    List<Object[]> parameters = new ArrayList<Object[]>();
    for (WriterVersion writerVersion : writerVersions) {
      for (PrimitiveTypeName type : types) {
        for (CompressionCodecName codec : codecs) {
          parameters.add(new Object[] {
              writerVersion,
              type,
              codec,
              60_000, // recordCount
              80, // columnCount
              100_000, // pageSize
              10_000_000, // rowGroupSize
              10_000_000, // dfsBlockSize
              1_000_000, // maxPaddingSize
          });
          parameters.add(new Object[] {
              writerVersion,
              type,
              codec,
              60_000, // recordCount
              80, // columnCount
              100_000, // pageSize
              5_000_000, // rowGroupSize
              10_000_000, // dfsBlockSize
              1_000_000 // maxPaddingSize
          });
        }
      }
    }

    return parameters;
  }

  public BlockAlignmentIT(
      WriterVersion writerVersion, PrimitiveTypeName typeName, CompressionCodecName compression,
      int recordCount, int columnCount, int pageSize,
      int rowGroupSize, int dfsBlockSize, int maxPaddingSize) {
    this.writerVersion = writerVersion;
    this.paramTypeName = typeName;
    this.compression = compression;
    this.recordCount = recordCount;
    this.columnCount = columnCount;
    this.pageSize = pageSize;
    this.rowGroupSize = rowGroupSize;
    this.dfsBlockSize = dfsBlockSize;
    this.maxPaddingSize = maxPaddingSize;
  }

  @BeforeClass
  public static void initialize() throws IOException {
    Random random = new Random(RANDOM_SEED);
    binaryGenerator = new RandomValues.BinaryGenerator(random.nextLong());
    fixedBinaryGenerator = new RandomValues.FixedGenerator(random.nextLong(), FIXED_LENGTH);
  }

  private void printParameters() {
    LOG.info("Testing {}/{}/{} encodings with the following settings:", writerVersion, paramTypeName, compression);
    LOG.info("  recordCount = {}, columnCount = {}, pageSize = {},", recordCount, columnCount, pageSize);
    LOG.info("  rowGroupSize = {}, dfsBlockSize = {}, maxPaddingSize = {}", rowGroupSize, dfsBlockSize, maxPaddingSize);
  }

  //@Test
  public void testFileEncodingsWithoutDictionary() throws Exception {
    printParameters();
    final boolean DISABLE_DICTIONARY = false;
    List<?> randomValues = generateRandomValues(this.paramTypeName, DISTINCT_VALUE_COUNT);

    Path parquetFile = createTempFile();
    writeValuesToFile(parquetFile, this.paramTypeName, randomValues, rowGroupSize, pageSize, DISABLE_DICTIONARY, writerVersion);
    validateAlignment(parquetFile);
  }

  @Test
  public void testFileEncodingsWithDictionary() throws Exception {
    printParameters();
    final boolean ENABLE_DICTIONARY = true;
    List<?> dictionaryValues = generateDictionaryValues(this.paramTypeName, DISTINCT_VALUE_COUNT);

    Path parquetFile = createTempFile();
    writeValuesToFile(parquetFile, this.paramTypeName, dictionaryValues, rowGroupSize, pageSize, ENABLE_DICTIONARY, writerVersion);
    validateAlignment(parquetFile);
  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private Path createTempFile() throws IOException {
    if (KEEP_TEMP_FILES) {
      return new Path("/tmp/BlockAlignmentITTempFile-" + UUID.randomUUID().toString());
    } else {
      File tempFile = tempFolder.newFile();
      tempFile.delete();
      return new Path(tempFile.getAbsolutePath());
    }
  }

  /**
   * Writes a set of values to a parquet file.
   * The ParquetWriter will write the values with dictionary encoding disabled so that we test specific encodings for
   */
  private void writeValuesToFile(Path file, PrimitiveTypeName type, List<?> values, int rowGroupSize, int pageSize, boolean enableDictionary, WriterVersion version) throws IOException {
    Configuration config = new Configuration();
    config.setInt("parquet.force-dfs-block-size", dfsBlockSize);

    MessageTypeBuilder schemaBuilder;
    schemaBuilder = Types.buildMessage();
    for (int colNo = 0; colNo < columnCount; ++colNo) {
      if (type == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
          schemaBuilder.required(type).length(FIXED_LENGTH).named(String.format("field%04d", colNo));
      } else {
        schemaBuilder.required(type).named(String.format("field%04d", colNo));
      }
    }
    MessageType schema = schemaBuilder.named("test");
    SimpleGroupFactory message = new SimpleGroupFactory(schema);
    GroupWriteSupport.setSchema(schema, config);

    ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withCompressionCodec(compression)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(pageSize)
        .withDictionaryPageSize(pageSize)
        .withDictionaryEncoding(enableDictionary)
        .withWriterVersion(version)
        .withConf(config)
        .withMaxPaddingSize(maxPaddingSize)
        .build();

    switch (type) {
    case BINARY:
    case FIXED_LEN_BYTE_ARRAY:
      for (int rowNo = 0; rowNo < recordCount; ++rowNo) {
        Group group = message.newGroup();
        for (int colNo = 0; colNo < columnCount; ++colNo) {
          group.append(String.format("field%04d", colNo),
              (Binary) values.get(rowNo % DISTINCT_VALUE_COUNT));
        }
        writer.write(group);
      }
      break;
    default:
      throw new IllegalArgumentException("Unknown type name: " + type);
    }

    writer.close();
  }

  private ArrayList<?> generateRandomValues(PrimitiveTypeName type, int count) {
    ArrayList<Object> values = new ArrayList<Object>();

    for (int i=0; i<count; i++) {
      Object value;
      switch (type) {
        case BINARY:
          value = binaryGenerator.nextBinaryValue();
        break;
        case FIXED_LEN_BYTE_ARRAY:
          value = fixedBinaryGenerator.nextBinaryValue();
        break;
        default:
          throw new IllegalArgumentException("Unknown type name: " + type);
      }

      values.add(value);
    }

    return values;
  }

  private ArrayList<?> generateDictionaryValues(PrimitiveTypeName type, int count) {
    final int DICT_VALUES_SIZE = DISTINCT_VALUE_COUNT;

    final List<?> DICT_BINARY_VALUES = generateRandomValues(PrimitiveTypeName.BINARY, DICT_VALUES_SIZE);
    final List<?> DICT_FIXED_LEN_VALUES = generateRandomValues(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, DICT_VALUES_SIZE);

    ArrayList<Object> values = new ArrayList<Object>();

    for (int i=0; i<count; i++) {
      int dictValue = i % DICT_VALUES_SIZE;
      Object value;
      switch (type) {
        case BINARY:
          value = DICT_BINARY_VALUES.get(dictValue);
          break;
        case FIXED_LEN_BYTE_ARRAY:
          value = DICT_FIXED_LEN_VALUES.get(dictValue);
          break;
        default:
          throw new IllegalArgumentException("Unknown type name: " + type);
      }

      values.add(value);
    }

    return values;
  }

  private void validateAlignment(Path file) throws IOException {
    ParquetMetadata metadata = ParquetFileReader.readFooter(new Configuration(), file, ParquetMetadataConverter.NO_FILTER);
    List<BlockMetaData> blocks = metadata.getBlocks();
    if (LOG.isInfoEnabled()) {
      for (int i = 0; i < blocks.size(); ++i) {
        BlockMetaData block = blocks.get(i);
        final long compressedSize = block.getCompressedSize();
        final long startingPos = block.getStartingPos();
        LOG.info("Block {} starts at byte {} and is {} bytes long.",
            String.format("%2d", i),
            String.format("%9d", startingPos),
            String.format("%8d", compressedSize));
      }
    }
    long nextDfsBlockStart = dfsBlockSize;
    for (int i = 0; i < blocks.size(); ++i) {
      BlockMetaData block = blocks.get(i);
      final long compressedSize = block.getCompressedSize();
      final long startingPos = block.getStartingPos();
      if (startingPos > nextDfsBlockStart) {
        fail(String.format("Row group %d starting at position %d is not properly aligned.", i, startingPos));
      } else if (startingPos == nextDfsBlockStart) {
        nextDfsBlockStart += dfsBlockSize;
      }
      assertTrue(String.format("Row group %d should not cross dfs block boundary.", i),
        startingPos + compressedSize <= nextDfsBlockStart);
      // If the row group size is the same as the DFS block size, we can check
      // an even stricter condition on all row groups except the last.
      if (rowGroupSize == dfsBlockSize && i < blocks.size() - 1) {
          assertTrue(String.format("Row group %d should end in a padding area.", i),
            startingPos + compressedSize >= nextDfsBlockStart - maxPaddingSize);
      }
    }
  }
}
