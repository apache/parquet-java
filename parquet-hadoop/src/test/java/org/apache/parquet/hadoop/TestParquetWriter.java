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

import static java.util.Arrays.asList;
import static org.apache.parquet.column.Encoding.DELTA_BYTE_ARRAY;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import net.openhft.hashing.LongHashFunction;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetrieverMock;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetWriter {

  /**
   * A test OutputFile implementation to validate the scenario of an OutputFile is implemented by an API client.
   */
  private static class TestOutputFile implements OutputFile {

    private final OutputFile outputFile;

    TestOutputFile(Path path, Configuration conf) throws IOException {
      outputFile = HadoopOutputFile.fromPath(path, conf);
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
      return outputFile.create(blockSizeHint);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
      return outputFile.createOrOverwrite(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
      return outputFile.supportsBlockSize();
    }

    @Override
    public long defaultBlockSize() {
      return outputFile.defaultBlockSize();
    }
  }

  private TrackingByteBufferAllocator allocator;

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    Path root = new Path("target/tests/TestParquetWriter/");
    enforceEmptyDir(conf, root);
    MessageType schema = parseMessageType("message test { "
        + "required binary binary_field; "
        + "required int32 int32_field; "
        + "required int64 int64_field; "
        + "required boolean boolean_field; "
        + "required float float_field; "
        + "required double double_field; "
        + "required fixed_len_byte_array(3) flba_field; "
        + "required int96 int96_field; "
        + "} ");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    Map<String, Encoding> expected = new HashMap<String, Encoding>();
    expected.put("10-" + PARQUET_1_0, PLAIN_DICTIONARY);
    expected.put("1000-" + PARQUET_1_0, PLAIN);
    expected.put("10-" + PARQUET_2_0, RLE_DICTIONARY);
    expected.put("1000-" + PARQUET_2_0, DELTA_BYTE_ARRAY);
    for (int modulo : asList(10, 1000)) {
      for (WriterVersion version : WriterVersion.values()) {
        Path file = new Path(root, version.name() + "_" + modulo);
        ParquetWriter<Group> writer = ExampleParquetWriter.builder(new TestOutputFile(file, conf))
            .withAllocator(allocator)
            .withCompressionCodec(UNCOMPRESSED)
            .withRowGroupSize(1024)
            .withPageSize(1024)
            .withDictionaryPageSize(512)
            .enableDictionaryEncoding()
            .withValidation(false)
            .withWriterVersion(version)
            .withConf(conf)
            .build();
        for (int i = 0; i < 1000; i++) {
          writer.write(f.newGroup()
              .append("binary_field", "test" + (i % modulo))
              .append("int32_field", 32)
              .append("int64_field", 64l)
              .append("boolean_field", true)
              .append("float_field", 1.0f)
              .append("double_field", 2.0d)
              .append("flba_field", "foo")
              .append("int96_field", Binary.fromConstantByteArray(new byte[12])));
        }
        writer.close();
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
            .withConf(conf)
            .build();
        for (int i = 0; i < 1000; i++) {
          Group group = reader.read();
          assertEquals(
              "test" + (i % modulo),
              group.getBinary("binary_field", 0).toStringUsingUTF8());
          assertEquals(32, group.getInteger("int32_field", 0));
          assertEquals(64l, group.getLong("int64_field", 0));
          assertEquals(true, group.getBoolean("boolean_field", 0));
          assertEquals(1.0f, group.getFloat("float_field", 0), 0.001);
          assertEquals(2.0d, group.getDouble("double_field", 0), 0.001);
          assertEquals("foo", group.getBinary("flba_field", 0).toStringUsingUTF8());
          assertEquals(Binary.fromConstantByteArray(new byte[12]), group.getInt96("int96_field", 0));
        }
        reader.close();
        ParquetMetadata footer = readFooter(conf, file, NO_FILTER);
        for (BlockMetaData blockMetaData : footer.getBlocks()) {
          for (ColumnChunkMetaData column : blockMetaData.getColumns()) {
            if (column.getPath().toDotString().equals("binary_field")) {
              String key = modulo + "-" + version;
              Encoding expectedEncoding = expected.get(key);
              assertTrue(
                  key + ":" + column.getEncodings() + " should contain " + expectedEncoding,
                  column.getEncodings().contains(expectedEncoding));
            }
          }
        }
        assertEquals(
            "Object model property should be example",
            "example",
            footer.getFileMetaData().getKeyValueMetaData().get(ParquetWriter.OBJECT_MODEL_NAME_PROP));
      }
    }
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testBadWriteSchema() throws IOException {
    final File file = temp.newFile("test.parquet");
    file.delete();

    TestUtils.assertThrows(
        "Should reject a schema with an empty group", InvalidSchemaException.class, (Callable<Void>) () -> {
          ExampleParquetWriter.builder(new Path(file.toString()))
              .withAllocator(allocator)
              .withType(Types.buildMessage()
                  .addField(new GroupType(REQUIRED, "invalid_group"))
                  .named("invalid_message"))
              .build();
          return null;
        });

    assertFalse("Should not create a file when schema is rejected", file.exists());
  }

  // Testing the issue of PARQUET-1531 where writing null nested rows leads to empty pages if the page row count limit
  // is reached.
  @Test
  public void testNullValuesWithPageRowLimit() throws IOException {
    MessageType schema = Types.buildMessage()
        .optionalList()
        .optionalElement(BINARY)
        .as(stringType())
        .named("str_list")
        .named("msg");
    final int recordCount = 100;
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    GroupFactory factory = new SimpleGroupFactory(schema);
    Group listNull = factory.newGroup();

    File file = temp.newFile();
    file.delete();
    Path path = new Path(file.getAbsolutePath());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withAllocator(allocator)
        .withPageRowCountLimit(10)
        .withConf(conf)
        .build()) {
      for (int i = 0; i < recordCount; ++i) {
        writer.write(listNull);
      }
    }

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), path).build()) {
      int readRecordCount = 0;
      for (Group group = reader.read(); group != null; group = reader.read()) {
        assertEquals(listNull.toString(), group.toString());
        ++readRecordCount;
      }
      assertEquals("Number of written records should be equal to the read one", recordCount, readRecordCount);
    }
  }

  @Test
  public void testParquetFileWithBloomFilter() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(stringType())
        .named("name")
        .named("msg");

    String[] testNames = {"hello", "parquet", "bloom", "filter"};
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    GroupFactory factory = new SimpleGroupFactory(schema);
    File file = temp.newFile();
    file.delete();
    Path path = new Path(file.getAbsolutePath());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withAllocator(allocator)
        .withPageRowCountLimit(10)
        .withConf(conf)
        .withDictionaryEncoding(false)
        .withBloomFilterEnabled("name", true)
        .build()) {
      for (String testName : testNames) {
        writer.write(factory.newGroup().append("name", testName));
      }
    }

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      BlockMetaData blockMetaData = reader.getFooter().getBlocks().get(0);
      BloomFilter bloomFilter = reader.getBloomFilterDataReader(blockMetaData)
          .readBloomFilter(blockMetaData.getColumns().get(0));

      for (String name : testNames) {
        assertTrue(bloomFilter.findHash(
            LongHashFunction.xx(0).hashBytes(Binary.fromString(name).toByteBuffer())));
      }
    }
  }

  @Test
  public void testParquetFileWithBloomFilterWithFpp() throws IOException {
    int buildBloomFilterCount = 100000;
    double[] testFpps = {0.01, 0.05, 0.10, 0.15, 0.20, 0.25};
    int randomStrLen = 12;
    final int testBloomFilterCount = 200000;

    Set<String> distinctStringsForFileGenerate = new HashSet<>();
    while (distinctStringsForFileGenerate.size() < buildBloomFilterCount) {
      String str = RandomStringUtils.randomAlphabetic(randomStrLen);
      distinctStringsForFileGenerate.add(str);
    }

    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(stringType())
        .named("name")
        .named("msg");

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    GroupFactory factory = new SimpleGroupFactory(schema);
    for (double testFpp : testFpps) {
      File file = temp.newFile();
      file.delete();
      Path path = new Path(file.getAbsolutePath());
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
          .withAllocator(allocator)
          .withPageRowCountLimit(10)
          .withConf(conf)
          .withDictionaryEncoding(false)
          .withBloomFilterEnabled("name", true)
          .withBloomFilterNDV("name", buildBloomFilterCount)
          .withBloomFilterFPP("name", testFpp)
          .build()) {
        for (String str : distinctStringsForFileGenerate) {
          writer.write(factory.newGroup().append("name", str));
        }
      }

      try (ParquetFileReader reader =
          ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
        BlockMetaData blockMetaData = reader.getFooter().getBlocks().get(0);
        BloomFilter bloomFilter = reader.getBloomFilterDataReader(blockMetaData)
            .readBloomFilter(blockMetaData.getColumns().get(0));

        // The false positive counts the number of times FindHash returns true.
        int falsePositive = 0;
        Set<String> distinctStringsForProbe = new HashSet<>();
        while (distinctStringsForProbe.size() < testBloomFilterCount) {
          String str = RandomStringUtils.randomAlphabetic(randomStrLen - 1);
          if (distinctStringsForProbe.add(str)
              && bloomFilter.findHash(LongHashFunction.xx(0)
                  .hashBytes(Binary.fromString(str).toByteBuffer()))) {
            falsePositive++;
          }
        }
        // The false positive should be less than totalCount * fpp. Add 15% here for error space.
        double expectedFalsePositiveMaxCount = Math.floor(testBloomFilterCount * (testFpp * 1.15));
        assertTrue(falsePositive < expectedFalsePositiveMaxCount && falsePositive > 0);
      }
    }
  }

  /**
   * If `parquet.bloom.filter.max.bytes` is set, the bytes size of bloom filter should not
   * be larger than this value
   */
  @Test
  public void testBloomFilterMaxBytesSize() throws IOException {
    Set<String> distinctStrings = new HashSet<>();
    while (distinctStrings.size() < 1000) {
      String str = RandomStringUtils.randomAlphabetic(10);
      distinctStrings.add(str);
    }
    int maxBloomFilterBytes = 1024 * 1024 + 1;
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(stringType())
        .named("name")
        .named("msg");
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);
    GroupFactory factory = new SimpleGroupFactory(schema);
    File file = temp.newFile();
    file.delete();
    Path path = new Path(file.getAbsolutePath());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withAllocator(allocator)
        .withConf(conf)
        .withDictionaryEncoding(false)
        .withBloomFilterEnabled("name", true)
        .withMaxBloomFilterBytes(maxBloomFilterBytes)
        .build()) {
      java.util.Iterator<String> iterator = distinctStrings.iterator();
      while (iterator.hasNext()) {
        writer.write(factory.newGroup().append("name", iterator.next()));
      }
    }
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      BlockMetaData blockMetaData = reader.getFooter().getBlocks().get(0);
      BloomFilter bloomFilter = reader.getBloomFilterDataReader(blockMetaData)
          .readBloomFilter(blockMetaData.getColumns().get(0));
      assertEquals(bloomFilter.getBitsetSize(), maxBloomFilterBytes);
    }
  }

  @Test
  public void testParquetFileWritesExpectedNumberOfBlocks() throws IOException {
    testParquetFileNumberOfBlocks(
        ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK,
        ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK,
        new Configuration(),
        1);
    testParquetFileNumberOfBlocks(1, 1, new Configuration(), 3);

    Configuration conf = new Configuration();
    ParquetOutputFormat.setBlockRowCountLimit(conf, 1);
    testParquetFileNumberOfBlocks(
        ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK,
        ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK,
        conf,
        3);
  }

  @Test
  public void testWriterConfigPersisted() throws Exception {
    Configuration conf = new Configuration();
    ParquetOutputFormat.setBlockRowCountLimit(conf, 2);
    ParquetOutputFormat.setPersistWriterConfig(conf, true);

    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(stringType())
        .named("str")
        .named("msg");
    GroupWriteSupport.setSchema(schema, conf);

    java.io.File file = new File(temp.getRoot(), "testWriterConfigPersisted.parquet");
    Path path = new Path(file.toURI());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withRowGroupRowCountLimit(2)
        .build()) {
      writer.write(new SimpleGroupFactory(schema).newGroup().append("str", "a"));
      writer.write(new SimpleGroupFactory(schema).newGroup().append("str", "b"));
    }

    try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
      java.util.Map<String, String> kv = r.getFooter().getFileMetaData().getKeyValueMetaData();
      Assert.assertEquals("2", kv.get(ParquetOutputFormat.BLOCK_ROW_COUNT_LIMIT));
    }
  }

  @Test
  public void testWriterConfigNotPersistedByDefault() throws Exception {
    Configuration conf = new Configuration();
    ParquetOutputFormat.setBlockRowCountLimit(conf, 2);

    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(stringType())
        .named("str")
        .named("msg");
    GroupWriteSupport.setSchema(schema, conf);

    java.io.File file = new File(temp.getRoot(), "testWriterConfigNotPersistedByDefault.parquet");
    Path path = new Path(file.toURI());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withRowGroupRowCountLimit(2)
        .build()) {
      writer.write(new SimpleGroupFactory(schema).newGroup().append("str", "a"));
      writer.write(new SimpleGroupFactory(schema).newGroup().append("str", "b"));
    }

    try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
      java.util.Map<String, String> kv = r.getFooter().getFileMetaData().getKeyValueMetaData();
      Assert.assertNull(
          "Writer config should not be persisted by default",
          kv.get(ParquetOutputFormat.BLOCK_ROW_COUNT_LIMIT));
    }
  }

  @Test
  public void testComprehensiveWriterConfigPersisted() throws Exception {
    Configuration conf = new Configuration();
    ParquetOutputFormat.setPersistWriterConfig(conf, true);

    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(stringType())
        .named("str")
        .named("msg");
    GroupWriteSupport.setSchema(schema, conf);

    java.io.File file = new File(temp.getRoot(), "testComprehensiveWriterConfigPersisted.parquet");
    Path path = new Path(file.toURI());

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withRowGroupRowCountLimit(1000)
        .withPageSize(1024)
        .withPageRowCountLimit(500)
        .withDictionaryPageSize(2048)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withDictionaryEncoding(true)
        .withMaxBloomFilterBytes(1024 * 1024)
        .withValidation(false)
        .withPageWriteChecksumEnabled(true)
        .withRowGroupSize(64 * 1024 * 1024)
        .withMaxPaddingSize(8 * 1024 * 1024)
        .build()) {
      writer.write(new SimpleGroupFactory(schema).newGroup().append("str", "test"));
    }

    try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
      java.util.Map<String, String> kv = r.getFooter().getFileMetaData().getKeyValueMetaData();

      Assert.assertEquals("1000", kv.get("parquet.block.row.count.limit"));
      Assert.assertEquals("67108864", kv.get("parquet.block.size"));
      Assert.assertEquals("1024", kv.get("parquet.page.size"));
      Assert.assertEquals("500", kv.get("parquet.page.row.count.limit"));
      Assert.assertEquals("2048", kv.get("parquet.dictionary.page.size"));
      Assert.assertEquals("SNAPPY", kv.get("parquet.compression.codec"));
      Assert.assertEquals("true", kv.get("parquet.dictionary.enabled"));
      Assert.assertEquals("1048576", kv.get("parquet.bloom.filter.max.bytes"));
      Assert.assertEquals("false", kv.get("parquet.validation.enabled"));
      Assert.assertEquals("true", kv.get("parquet.page.write.checksum.enabled"));
      Assert.assertEquals("8388608", kv.get("parquet.max.padding.size"));
    }
  }

  @Test
  public void testExtraMetaData() throws Exception {
    final Configuration conf = new Configuration();
    final File testDir = temp.newFile();
    testDir.delete();

    final MessageType schema = parseMessageType("message test { required int32 int32_field; }");
    GroupWriteSupport.setSchema(schema, conf);
    final SimpleGroupFactory f = new SimpleGroupFactory(schema);

    for (WriterVersion version : WriterVersion.values()) {
      final Path filePath = new Path(testDir.getAbsolutePath(), version.name());
      final ParquetWriter<Group> writer = ExampleParquetWriter.builder(new TestOutputFile(filePath, conf))
          .withConf(conf)
          .withExtraMetaData(ImmutableMap.of("simple-key", "some-value-1", "nested.key", "some-value-2"))
          .build();
      for (int i = 0; i < 1000; i++) {
        writer.write(f.newGroup().append("int32_field", 32));
      }
      writer.close();

      final ParquetFileReader reader =
          ParquetFileReader.open(HadoopInputFile.fromPath(filePath, new Configuration()));
      assertEquals(1000, reader.readNextRowGroup().getRowCount());
      assertEquals(
          ImmutableMap.of(
              "simple-key",
              "some-value-1",
              "nested.key",
              "some-value-2",
              ParquetWriter.OBJECT_MODEL_NAME_PROP,
              "example"),
          reader.getFileMetaData().getKeyValueMetaData());

      reader.close();
    }
  }

  @Test
  public void testFailsOnConflictingExtraMetaDataKey() throws Exception {
    final Configuration conf = new Configuration();
    final File testDir = temp.newFile();
    testDir.delete();

    final MessageType schema = parseMessageType("message test { required int32 int32_field; }");
    GroupWriteSupport.setSchema(schema, conf);

    for (WriterVersion version : WriterVersion.values()) {
      final Path filePath = new Path(testDir.getAbsolutePath(), version.name());

      Assert.assertThrows(IllegalArgumentException.class, () -> ExampleParquetWriter.builder(
              new TestOutputFile(filePath, conf))
          .withConf(conf)
          .withExtraMetaData(ImmutableMap.of(ParquetWriter.OBJECT_MODEL_NAME_PROP, "some-value-3"))
          .build());
    }
  }

  private void testParquetFileNumberOfBlocks(
      int minRowCountForPageSizeCheck,
      int maxRowCountForPageSizeCheck,
      Configuration conf,
      int expectedNumberOfBlocks)
      throws IOException {
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(stringType())
        .named("str")
        .named("msg");

    GroupWriteSupport.setSchema(schema, conf);

    File file = temp.newFile();
    temp.delete();
    Path path = new Path(file.getAbsolutePath());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withAllocator(allocator)
        .withConf(conf)
        .withRowGroupRowCountLimit(ParquetOutputFormat.getBlockRowCountLimit(conf))
        // Set row group size to 1, to make sure we flush every time when
        // minRowCountForPageSizeCheck or maxRowCountForPageSizeCheck is exceeded
        .withRowGroupSize(1)
        .withMinRowCountForPageSizeCheck(minRowCountForPageSizeCheck)
        .withMaxRowCountForPageSizeCheck(maxRowCountForPageSizeCheck)
        .build()) {

      SimpleGroupFactory factory = new SimpleGroupFactory(schema);
      writer.write(factory.newGroup().append("str", "foo"));
      writer.write(factory.newGroup().append("str", "bar"));
      writer.write(factory.newGroup().append("str", "baz"));
    }

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
      ParquetMetadata footer = reader.getFooter();
      assertEquals(expectedNumberOfBlocks, footer.getBlocks().size());
    }
  }

  @Test
  public void testSizeStatisticsAndStatisticsControl() throws Exception {
    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .named("string_field")
        .required(BOOLEAN)
        .named("boolean_field")
        .required(INT32)
        .named("int32_field")
        .named("test_schema");

    SimpleGroupFactory factory = new SimpleGroupFactory(schema);

    // Create test data
    Group group = factory.newGroup()
        .append("string_field", "test")
        .append("boolean_field", true)
        .append("int32_field", 42);

    // Test global disable
    File file = temp.newFile();
    temp.delete();
    Path path = new Path(file.getAbsolutePath());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(schema)
        .withSizeStatisticsEnabled(false)
        .withStatisticsEnabled(false) // Disable column statistics globally
        .build()) {
      writer.write(group);
    }

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      // Verify size statistics are disabled globally
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          assertTrue(column.getStatistics().isEmpty()); // Make sure there is no column statistics
          assertNull(column.getSizeStatistics());
        }
      }
    }

    // Test column-specific control
    file = temp.newFile();
    temp.delete();
    path = new Path(file.getAbsolutePath());
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(schema)
        .withSizeStatisticsEnabled(true) // enable globally
        .withSizeStatisticsEnabled("boolean_field", false) // disable for specific column
        .withStatisticsEnabled("boolean_field", false) // disable column statistics
        .build()) {
      writer.write(group);
    }

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      // Verify size statistics are enabled for all columns except boolean_field
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          if (column.getPath().toDotString().equals("boolean_field")) {
            assertNull(column.getSizeStatistics());
            assertTrue(column.getStatistics().isEmpty());
          } else {
            assertTrue(column.getSizeStatistics().isValid());
            assertFalse(column.getStatistics().isEmpty());
          }
        }
      }
    }
  }

  @Test
  public void testV2WriteAllNullValues() throws Exception {
    testV2WriteAllNullValues(null, null);
  }

  @Test
  public void testV2WriteAllNullValuesWithEncrypted() throws Exception {
    byte[] footerEncryptionKey = "0123456789012345".getBytes();
    byte[] columnEncryptionKey = "1234567890123450".getBytes();

    String footerEncryptionKeyID = "kf";
    String columnEncryptionKeyID = "kc";

    ColumnEncryptionProperties columnProperties = ColumnEncryptionProperties.builder("float")
        .withKey(columnEncryptionKey)
        .withKeyID(columnEncryptionKeyID)
        .build();

    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap = new HashMap<>();
    columnPropertiesMap.put(columnProperties.getPath(), columnProperties);

    FileEncryptionProperties encryptionProperties = FileEncryptionProperties.builder(footerEncryptionKey)
        .withFooterKeyID(footerEncryptionKeyID)
        .withEncryptedColumns(columnPropertiesMap)
        .build();

    DecryptionKeyRetrieverMock decryptionKeyRetrieverMock = new DecryptionKeyRetrieverMock()
        .putKey(footerEncryptionKeyID, footerEncryptionKey)
        .putKey(columnEncryptionKeyID, columnEncryptionKey);
    FileDecryptionProperties decryptionProperties = FileDecryptionProperties.builder()
        .withKeyRetriever(decryptionKeyRetrieverMock)
        .build();

    testV2WriteAllNullValues(encryptionProperties, decryptionProperties);
  }

  private void testV2WriteAllNullValues(
      FileEncryptionProperties encryptionProperties, FileDecryptionProperties decryptionProperties)
      throws Exception {
    MessageType schema = Types.buildMessage().optional(FLOAT).named("float").named("msg");

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    File file = temp.newFile();
    temp.delete();
    Path path = new Path(file.getAbsolutePath());

    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    Group nullValue = factory.newGroup();
    int recordCount = 10;

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withAllocator(allocator)
        .withConf(conf)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withDictionaryEncoding(false)
        .withEncryption(encryptionProperties)
        .build()) {
      for (int i = 0; i < recordCount; i++) {
        writer.write(nullValue);
      }
    }

    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
        .withDecryption(decryptionProperties)
        .build()) {
      int readRecordCount = 0;
      for (Group group = reader.read(); group != null; group = reader.read()) {
        assertEquals(nullValue.toString(), group.toString());
        ++readRecordCount;
      }
      assertEquals("Number of written records should be equal to the read one", recordCount, readRecordCount);
    }

    ParquetReadOptions options = ParquetReadOptions.builder()
        .withDecryption(decryptionProperties)
        .build();
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf), options)) {
      BlockMetaData blockMetaData = reader.getFooter().getBlocks().get(0);
      reader.f.seek(blockMetaData.getStartingPos());

      if (decryptionProperties != null) {
        InternalFileDecryptor fileDecryptor =
            reader.getFooter().getFileMetaData().getFileDecryptor();
        InternalColumnDecryptionSetup columnDecryptionSetup =
            fileDecryptor.getColumnSetup(ColumnPath.fromDotString("float"));
        byte[] dataPageHeaderAAD = AesCipher.createModuleAAD(
            fileDecryptor.getFileAAD(), ModuleCipherFactory.ModuleType.DataPageHeader, 0, 0, 0);
        PageHeader pageHeader =
            Util.readPageHeader(reader.f, columnDecryptionSetup.getMetaDataDecryptor(), dataPageHeaderAAD);
        assertFalse(pageHeader.getData_page_header_v2().isIs_compressed());
      } else {
        PageHeader pageHeader = Util.readPageHeader(reader.f);
        assertFalse(pageHeader.getData_page_header_v2().isIs_compressed());
      }
    }
  }
}
