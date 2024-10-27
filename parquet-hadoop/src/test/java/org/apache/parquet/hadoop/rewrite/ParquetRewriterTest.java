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

import static java.util.Collections.emptyMap;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Version;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.IndexCache;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.EncDecProperties;
import org.apache.parquet.hadoop.util.EncryptionTestFile;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.hadoop.util.TestFileBuilder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ParquetRewriterTest {

  private final int numRecord = 100000;
  private final Configuration conf = new Configuration();
  private final ParquetConfiguration parquetConf = new PlainParquetConfiguration();
  private final ParquetProperties.WriterVersion writerVersion;
  private final IndexCache.CacheStrategy indexCacheStrategy;
  private final boolean usingHadoop;

  private List<EncryptionTestFile> inputFiles = Lists.newArrayList();
  private List<EncryptionTestFile> inputFilesToJoin = Lists.newArrayList();
  private String outputFile = null;
  private ParquetRewriter rewriter = null;

  private final EncryptionTestFile gzipEncryptionTestFileWithoutBloomFilterColumn;
  private final EncryptionTestFile uncompressedEncryptionTestFileWithoutBloomFilterColumn;

  @Parameterized.Parameters(name = "WriterVersion = {0}, IndexCacheStrategy = {1}, UsingHadoop = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"v1", "NONE", true},
      {"v1", "PREFETCH_BLOCK", true},
      {"v2", "PREFETCH_BLOCK", true},
      {"v2", "PREFETCH_BLOCK", false}
    };
  }

  public ParquetRewriterTest(String writerVersion, String indexCacheStrategy, boolean usingHadoop)
      throws IOException {
    this.writerVersion = ParquetProperties.WriterVersion.fromString(writerVersion);
    this.indexCacheStrategy = IndexCache.CacheStrategy.valueOf(indexCacheStrategy);
    this.usingHadoop = usingHadoop;

    MessageType testSchema = createSchema();
    this.gzipEncryptionTestFileWithoutBloomFilterColumn = new TestFileBuilder(conf, testSchema)
        .withNumRecord(numRecord)
        .withCodec("GZIP")
        .withPageSize(1024)
        .withWriterVersion(this.writerVersion)
        .build();

    this.uncompressedEncryptionTestFileWithoutBloomFilterColumn = new TestFileBuilder(conf, testSchema)
        .withNumRecord(numRecord)
        .withCodec("UNCOMPRESSED")
        .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
        .withWriterVersion(this.writerVersion)
        .build();
  }

  private void testPruneSingleColumnTranslateCodec(List<Path> inputPaths) throws Exception {
    RewriteOptions.Builder builder = createBuilder(inputPaths);

    List<String> pruneColumns = Collections.singletonList("Gender");
    CompressionCodecName newCodec = CompressionCodecName.ZSTD;
    RewriteOptions options = builder.prune(pruneColumns)
        .transform(newCodec)
        .indexCacheStrategy(indexCacheStrategy)
        .build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema is not changed for the columns not pruned
    validateSchemaWithGenderColumnPruned(false);

    // Verify codec has been translated
    verifyCodec(
        outputFile,
        new HashSet<CompressionCodecName>() {
          {
            add(CompressionCodecName.ZSTD);
          }
        },
        null);

    // Verify the data are not changed for the columns not pruned
    validateColumnData(new HashSet<>(pruneColumns), Collections.emptySet(), null, false, emptyMap());

    // Verify the page index
    validatePageIndex(new HashSet<>(), false, emptyMap());

    // Verify original.created.by is preserved
    validateCreatedBy();
    validateRowGroupRowCount();
  }

  @Before
  public void setUp() {
    outputFile = TestFileBuilder.createTempFile("test");
    inputFilesToJoin = new ArrayList<>();
  }

  @Test
  public void testPruneSingleColumnTranslateCodecSingleFile() throws Exception {
    addGzipInputFile();
    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
      }
    };
    testPruneSingleColumnTranslateCodec(inputPaths);
  }

  @Test
  public void testPruneSingleColumnTranslateCodecTwoFiles() throws Exception {
    addGzipInputFile();
    addUncompressedInputFile();
    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
        add(new Path(inputFiles.get(1).getFileName()));
      }
    };
    testPruneSingleColumnTranslateCodec(inputPaths);
  }

  private void testPruneNullifyTranslateCodec(List<Path> inputPaths) throws Exception {
    RewriteOptions.Builder builder = createBuilder(inputPaths);

    List<String> pruneColumns = Collections.singletonList("Gender");
    Map<String, MaskMode> maskColumns = new HashMap<>();
    maskColumns.put("Links.Forward", MaskMode.NULLIFY);
    CompressionCodecName newCodec = CompressionCodecName.ZSTD;
    RewriteOptions options = builder.prune(pruneColumns)
        .mask(maskColumns)
        .transform(newCodec)
        .indexCacheStrategy(indexCacheStrategy)
        .build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema are not changed for the columns not pruned
    validateSchemaWithGenderColumnPruned(false);

    // Verify codec has been translated
    verifyCodec(
        outputFile,
        new HashSet<CompressionCodecName>() {
          {
            add(newCodec);
          }
        },
        null);

    // Verify the data are not changed for the columns not pruned
    validateColumnData(new HashSet<>(pruneColumns), maskColumns.keySet(), null, false, emptyMap());

    // Verify the page index
    validatePageIndex(ImmutableSet.of("Links.Forward"), false, emptyMap());

    // Verify original.created.by is preserved
    validateCreatedBy();
    validateRowGroupRowCount();
  }

  @Test
  public void testPruneNullifyTranslateCodecSingleFile() throws Exception {
    addGzipInputFile();

    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
      }
    };
    testPruneNullifyTranslateCodec(inputPaths);
  }

  @Test
  public void testPruneNullifyTranslateCodecTwoFiles() throws Exception {
    addGzipInputFile();
    addUncompressedInputFile();

    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
        add(new Path(inputFiles.get(1).getFileName()));
      }
    };
    testPruneNullifyTranslateCodec(inputPaths);
  }

  private void testPruneEncryptTranslateCodec(List<Path> inputPaths) throws Exception {
    RewriteOptions.Builder builder = createBuilder(inputPaths);

    // Prune
    List<String> pruneColumns = Collections.singletonList("Gender");
    builder.prune(pruneColumns);

    // Translate codec
    CompressionCodecName newCodec = CompressionCodecName.ZSTD;
    builder.transform(newCodec);

    // Encrypt
    String[] encryptColumns = {"DocId"};
    FileEncryptionProperties fileEncryptionProperties =
        EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, false);
    builder.encrypt(Arrays.asList(encryptColumns)).encryptionProperties(fileEncryptionProperties);

    builder.indexCacheStrategy(indexCacheStrategy);

    RewriteOptions options = builder.build();
    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema is not changed for the columns not pruned
    validateSchemaWithGenderColumnPruned(false);

    // Verify codec has been translated
    FileDecryptionProperties fileDecryptionProperties = EncDecProperties.getFileDecryptionProperties();
    verifyCodec(
        outputFile,
        new HashSet<CompressionCodecName>() {
          {
            add(newCodec);
          }
        },
        fileDecryptionProperties);

    // Verify the data are not changed for the columns not pruned
    validateColumnData(
        new HashSet<>(pruneColumns), Collections.emptySet(), fileDecryptionProperties, false, emptyMap());

    // Verify column encryption
    ParquetMetadata metaData = getFileMetaData(outputFile, fileDecryptionProperties);
    assertFalse(metaData.getBlocks().isEmpty());
    List<ColumnChunkMetaData> columns = metaData.getBlocks().get(0).getColumns();
    Set<String> set = new HashSet<>(Arrays.asList(encryptColumns));
    for (ColumnChunkMetaData column : columns) {
      if (set.contains(column.getPath().toDotString())) {
        assertTrue(column.isEncrypted());
      } else {
        assertFalse(column.isEncrypted());
      }
    }

    // Verify original.created.by is preserved
    validateCreatedBy();
    validateRowGroupRowCount();
  }

  @Test
  public void testPruneEncryptTranslateCodecSingleFile() throws Exception {
    addGzipInputFile();

    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
      }
    };
    testPruneEncryptTranslateCodec(inputPaths);
  }

  @Test
  public void testPruneEncryptTranslateCodecTwoFiles() throws Exception {
    addGzipInputFile();
    addUncompressedInputFile();

    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
        add(new Path(inputFiles.get(1).getFileName()));
      }
    };
    testPruneEncryptTranslateCodec(inputPaths);
  }

  @Test
  public void testRewriteWithoutColumnIndexes() throws Exception {
    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(ParquetRewriterTest.class
            .getResource("/test-file-with-no-column-indexes-1.parquet")
            .toURI()));
      }
    };

    inputFiles = inputPaths.stream()
        .map(p -> new EncryptionTestFile(p.toString(), null))
        .collect(Collectors.toList());

    RewriteOptions.Builder builder = createBuilder(inputPaths);

    Map<String, MaskMode> maskCols = Maps.newHashMap();
    maskCols.put("location.lat", MaskMode.NULLIFY);
    maskCols.put("location.lon", MaskMode.NULLIFY);
    maskCols.put("location", MaskMode.NULLIFY);

    List<String> pruneCols = Lists.newArrayList("phoneNumbers");

    RewriteOptions options = builder.mask(maskCols)
        .prune(pruneCols)
        .indexCacheStrategy(indexCacheStrategy)
        .build();
    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema is not changed for the columns not pruned
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertEquals(fields.size(), 3);
    assertEquals(fields.get(0).getName(), "id");
    assertEquals(fields.get(1).getName(), "name");
    assertEquals(fields.get(2).getName(), "location");
    List<Type> subFields = fields.get(2).asGroupType().getFields();
    assertEquals(subFields.size(), 2);
    assertEquals(subFields.get(0).getName(), "lon");
    assertEquals(subFields.get(1).getName(), "lat");

    try (ParquetReader<Group> outReader = ParquetReader.builder(new GroupReadSupport(), new Path(outputFile))
            .withConf(conf)
            .build();
        ParquetReader<Group> inReader = ParquetReader.builder(new GroupReadSupport(), inputPaths.get(0))
            .withConf(conf)
            .build(); ) {

      for (Group inRead = inReader.read(), outRead = outReader.read();
          inRead != null || outRead != null;
          inRead = inReader.read(), outRead = outReader.read()) {
        assertNotNull(inRead);
        assertNotNull(outRead);

        assertEquals(inRead.getLong("id", 0), outRead.getLong("id", 0));
        assertEquals(inRead.getString("name", 0), outRead.getString("name", 0));

        // location was null
        Group finalOutRead = outRead;
        assertThrows(
            RuntimeException.class,
            () -> finalOutRead.getGroup("location", 0).getDouble("lat", 0));
        assertThrows(
            RuntimeException.class,
            () -> finalOutRead.getGroup("location", 0).getDouble("lon", 0));

        // phone numbers was pruned
        assertThrows(InvalidRecordException.class, () -> finalOutRead.getGroup("phoneNumbers", 0));
      }
    }

    // Verify original.created.by is preserved
    validateCreatedBy();
    validateRowGroupRowCount();
  }

  private void testNullifyAndEncryptColumn(List<Path> inputPaths) throws Exception {
    Map<String, MaskMode> maskColumns = new HashMap<>();
    maskColumns.put("Links.Forward", MaskMode.NULLIFY);

    String[] encryptColumns = {"DocId"};
    FileEncryptionProperties fileEncryptionProperties =
        EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, false);

    RewriteOptions.Builder builder = createBuilder(inputPaths);

    RewriteOptions options = builder.mask(maskColumns)
        .transform(CompressionCodecName.ZSTD)
        .encrypt(Arrays.asList(encryptColumns))
        .encryptionProperties(fileEncryptionProperties)
        .indexCacheStrategy(indexCacheStrategy)
        .build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    FileDecryptionProperties fileDecryptionProperties = EncDecProperties.getFileDecryptionProperties();

    // Verify codec has not been changed
    verifyCodec(
        outputFile,
        new HashSet<CompressionCodecName>() {
          {
            add(CompressionCodecName.ZSTD);
          }
        },
        fileDecryptionProperties);

    // Verify the data are not changed for non-encrypted and non-masked columns.
    // Also make sure the masked column is nullified.
    validateColumnData(Collections.emptySet(), maskColumns.keySet(), fileDecryptionProperties, false, emptyMap());

    // Verify the page index
    validatePageIndex(ImmutableSet.of("DocId", "Links.Forward"), false, emptyMap());

    // Verify the column is encrypted
    ParquetMetadata metaData = getFileMetaData(outputFile, fileDecryptionProperties);
    assertFalse(metaData.getBlocks().isEmpty());
    Set<String> encryptedColumns = new HashSet<>(Arrays.asList(encryptColumns));
    for (BlockMetaData blockMetaData : metaData.getBlocks()) {
      List<ColumnChunkMetaData> columns = blockMetaData.getColumns();
      for (ColumnChunkMetaData column : columns) {
        if (encryptedColumns.contains(column.getPath().toDotString())) {
          assertTrue(column.isEncrypted());
        } else {
          assertFalse(column.isEncrypted());
        }
      }
    }
  }

  @Test
  public void testNullifyEncryptSingleFile() throws Exception {
    addGzipInputFile();

    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
      }
    };
    testNullifyAndEncryptColumn(inputPaths);
  }

  @Test
  public void testNullifyEncryptTwoFiles() throws Exception {
    addGzipInputFile();
    addUncompressedInputFile();

    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
        add(new Path(inputFiles.get(1).getFileName()));
      }
    };
    testNullifyAndEncryptColumn(inputPaths);
  }

  @Test
  public void testMergeTwoFilesOnly() throws Exception {
    addGzipInputFile();
    addUncompressedInputFile();

    // Only merge two files but do not change anything.
    List<Path> inputPaths = new ArrayList<>();
    for (EncryptionTestFile inputFile : inputFiles) {
      inputPaths.add(new Path(inputFile.getFileName()));
    }
    RewriteOptions.Builder builder = createBuilder(inputPaths);
    RewriteOptions options = builder.indexCacheStrategy(indexCacheStrategy).build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema is not changed
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    MessageType expectSchema = createSchema();
    assertEquals(expectSchema, schema);

    // Verify codec has not been translated
    verifyCodec(
        outputFile,
        new HashSet<CompressionCodecName>() {
          {
            add(CompressionCodecName.GZIP);
            add(CompressionCodecName.UNCOMPRESSED);
          }
        },
        null);

    // Verify the merged data are not changed
    validateColumnData(Collections.emptySet(), Collections.emptySet(), null, false, emptyMap());

    // Verify the page index
    validatePageIndex(new HashSet<>(), false, emptyMap());

    // Verify original.created.by is preserved
    validateCreatedBy();
    validateRowGroupRowCount();
  }

  @Test
  public void testMergeTwoFilesOnlyRenameColumn() throws Exception {
    addGzipInputFile();
    addUncompressedInputFile();

    // Only merge two files but do not change anything.
    List<Path> inputPaths = new ArrayList<>();
    for (EncryptionTestFile inputFile : inputFiles) {
      inputPaths.add(new Path(inputFile.getFileName()));
    }
    Map<String, String> renameColumns = ImmutableMap.of("Name", "NameRenamed");
    RewriteOptions.Builder builder = createBuilder(inputPaths);
    RewriteOptions options = builder.indexCacheStrategy(indexCacheStrategy)
        .renameColumns(ImmutableMap.of("Name", "NameRenamed"))
        .build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema is not changed
    ParquetMetadata pmd =
        ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    MessageType expectSchema = createSchemaWithRenamed();
    assertEquals(expectSchema, schema);

    // Verify codec has not been translated
    verifyCodec(
        outputFile,
        new HashSet<CompressionCodecName>() {
          {
            add(CompressionCodecName.GZIP);
            add(CompressionCodecName.UNCOMPRESSED);
          }
        },
        null);

    // Verify the merged data are not changed
    validateColumnData(Collections.emptySet(), Collections.emptySet(), null, false, renameColumns);

    // Verify the page index
    validatePageIndex(new HashSet<>(), false, renameColumns);

    // Verify original.created.by is preserved
    validateCreatedBy();
    validateRowGroupRowCount();
  }

  @Test(expected = InvalidSchemaException.class)
  public void testMergeTwoFilesWithDifferentSchema() throws Exception {
    testMergeTwoFilesWithDifferentSchemaSetup(true);
  }

  @Test(expected = InvalidSchemaException.class)
  public void testMergeTwoFilesToJoinWithDifferentSchema() throws Exception {
    testMergeTwoFilesWithDifferentSchemaSetup(false);
  }

  public void testMergeTwoFilesWithDifferentSchemaSetup(boolean wrongSchemaInInputFile) throws Exception {
    MessageType schema1 = new MessageType(
        "schema",
        new PrimitiveType(OPTIONAL, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new PrimitiveType(OPTIONAL, BINARY, "Gender"),
        new GroupType(
            OPTIONAL,
            "Links",
            new PrimitiveType(REPEATED, BINARY, "Backward"),
            new PrimitiveType(REPEATED, BINARY, "Forward")));
    MessageType schema2 = new MessageType(
        "schema",
        new PrimitiveType(OPTIONAL, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new PrimitiveType(OPTIONAL, BINARY, "Gender"));
    inputFiles = Lists.newArrayList();
    inputFiles.add(new TestFileBuilder(conf, schema1)
        .withNumRecord(numRecord)
        .withCodec("UNCOMPRESSED")
        .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
        .withWriterVersion(writerVersion)
        .build());
    inputFilesToJoin.add(new TestFileBuilder(conf, schema1)
        .withNumRecord(numRecord)
        .withCodec("UNCOMPRESSED")
        .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
        .withWriterVersion(writerVersion)
        .build());
    if (wrongSchemaInInputFile) {
      inputFiles.add(new TestFileBuilder(conf, schema2)
          .withNumRecord(numRecord)
          .withCodec("UNCOMPRESSED")
          .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
          .withWriterVersion(writerVersion)
          .build());
    } else {
      inputFilesToJoin.add(new TestFileBuilder(conf, schema2)
          .withNumRecord(numRecord)
          .withCodec("UNCOMPRESSED")
          .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
          .withWriterVersion(writerVersion)
          .build());
    }

    RewriteOptions.Builder builder = createBuilder(
        inputFiles.stream().map(x -> new Path(x.getFileName())).collect(Collectors.toList()),
        inputFilesToJoin.stream().map(x -> new Path(x.getFileName())).collect(Collectors.toList()),
        false);
    RewriteOptions options = builder.indexCacheStrategy(indexCacheStrategy).build();

    // This should throw an exception because the schemas are different
    rewriter = new ParquetRewriter(options);
  }

  @Test
  public void testRewriteFileWithMultipleBlocks() throws Exception {
    addGzipInputFile();

    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
      }
    };
    testPruneSingleColumnTranslateCodec(inputPaths);
  }

  @Test
  public void testPruneSingleColumnTranslateCodecAndEnableBloomFilter() throws Exception {
    testSingleInputFileSetupWithBloomFilter("DocId");
    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
      }
    };
    testPruneSingleColumnTranslateCodec(inputPaths);

    // Verify bloom filters
    Map<ColumnPath, List<BloomFilter>> inputBloomFilters = allInputBloomFilters();
    Map<ColumnPath, List<BloomFilter>> outputBloomFilters = allOutputBloomFilters(null);
    assertEquals(inputBloomFilters, outputBloomFilters);
  }

  @Test
  public void testPruneNullifyTranslateCodecAndEnableBloomFilter() throws Exception {
    testSingleInputFileSetupWithBloomFilter("DocId", "Links.Forward");
    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
      }
    };
    testPruneNullifyTranslateCodec(inputPaths);

    // Verify bloom filters
    Map<ColumnPath, List<BloomFilter>> inputBloomFilters = allInputBloomFilters();
    assertEquals(inputBloomFilters.size(), 2);
    assertTrue(inputBloomFilters.containsKey(ColumnPath.fromDotString("Links.Forward")));
    assertTrue(inputBloomFilters.containsKey(ColumnPath.fromDotString("DocId")));

    Map<ColumnPath, List<BloomFilter>> outputBloomFilters = allOutputBloomFilters(null);
    assertEquals(outputBloomFilters.size(), 1);
    assertTrue(outputBloomFilters.containsKey(ColumnPath.fromDotString("DocId")));

    inputBloomFilters.remove(ColumnPath.fromDotString("Links.Forward"));
    assertEquals(inputBloomFilters, outputBloomFilters);
  }

  @Test
  public void testPruneEncryptTranslateCodecAndEnableBloomFilter() throws Exception {
    testSingleInputFileSetupWithBloomFilter("DocId", "Links.Forward");
    List<Path> inputPaths = new ArrayList<Path>() {
      {
        add(new Path(inputFiles.get(0).getFileName()));
      }
    };
    testPruneEncryptTranslateCodec(inputPaths);

    // Verify bloom filters
    Map<ColumnPath, List<BloomFilter>> inputBloomFilters = allInputBloomFilters();

    // Cannot read without FileDecryptionProperties
    assertThrows(ParquetCryptoRuntimeException.class, () -> allOutputBloomFilters(null));

    FileDecryptionProperties fileDecryptionProperties = EncDecProperties.getFileDecryptionProperties();
    Map<ColumnPath, List<BloomFilter>> outputBloomFilters = allOutputBloomFilters(fileDecryptionProperties);
    assertEquals(inputBloomFilters, outputBloomFilters);
  }

  private void testSingleInputFileSetupWithBloomFilter(String... bloomFilterEnabledColumns) throws IOException {
    testSingleInputFileSetup(bloomFilterEnabledColumns);
  }

  private void testSingleInputFileSetup(String... bloomFilterEnabledColumns) throws IOException {
    MessageType schema = createSchema();
    inputFiles = Lists.newArrayList();
    inputFiles.add(new TestFileBuilder(conf, schema)
        .withNumRecord(numRecord)
        .withCodec("GZIP")
        .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
        .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
        .withBloomFilterEnabled(bloomFilterEnabledColumns)
        .withWriterVersion(writerVersion)
        .build());
  }

  @Test
  public void testFilesToJoinHaveDifferentRowCount() throws Exception {
    MessageType schema1 = new MessageType("schema", new PrimitiveType(OPTIONAL, INT64, "DocId"));
    MessageType schema2 = new MessageType("schema", new PrimitiveType(REQUIRED, BINARY, "Name"));
    inputFiles = ImmutableList.of(
        new TestFileBuilder(conf, schema1).withNumRecord(numRecord).build());
    inputFilesToJoin = ImmutableList.of(
        new TestFileBuilder(conf, schema2).withNumRecord(numRecord / 2).build());
    RewriteOptions.Builder builder = createBuilder(
        inputFiles.stream().map(x -> new Path(x.getFileName())).collect(Collectors.toList()),
        inputFilesToJoin.stream().map(x -> new Path(x.getFileName())).collect(Collectors.toList()),
        true);
    RewriteOptions options = builder.build();
    try {
      rewriter =
          new ParquetRewriter(options); // This should throw an exception because the row count is different
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("The number of rows in each block must match"));
    }
  }

  @Test
  public void testOneInputFileManyInputFilesToJoinWithJoinColumnsOverwrite() throws Exception {
    testOneInputFileManyInputFilesToJoinSetup(true);
  }

  @Test
  public void testOneInputFileManyInputFilesToJoinWithoutJoinColumnsOverwrite() throws Exception {
    testOneInputFileManyInputFilesToJoinSetup(false);
  }

  public void testOneInputFileManyInputFilesToJoinSetup(boolean joinColumnsOverwrite) throws Exception {
    testOneInputFileManyInputFilesToJoinSetup();

    String encryptColumn = "DocId";
    String pruneColumn = "Gender";

    FileEncryptionProperties fileEncryptionProperties = EncDecProperties.getFileEncryptionProperties(
        new String[] {encryptColumn}, ParquetCipher.AES_GCM_CTR_V1, false);
    FileDecryptionProperties fileDecryptionProperties = EncDecProperties.getFileDecryptionProperties();

    List<Path> inputPathsL =
        inputFiles.stream().map(x -> new Path(x.getFileName())).collect(Collectors.toList());
    List<Path> inputPathsR =
        inputFilesToJoin.stream().map(y -> new Path(y.getFileName())).collect(Collectors.toList());
    List<String> pruneColumns = ImmutableList.of(pruneColumn);
    Map<String, MaskMode> maskColumns = ImmutableMap.of(encryptColumn, MaskMode.NULLIFY);
    RewriteOptions options = createBuilder(inputPathsL, inputPathsR, true)
        .prune(pruneColumns)
        .mask(maskColumns)
        .transform(CompressionCodecName.ZSTD)
        .indexCacheStrategy(indexCacheStrategy)
        .overwriteInputWithJoinColumns(joinColumnsOverwrite)
        .encrypt(ImmutableList.of(encryptColumn))
        .encryptionProperties(fileEncryptionProperties)
        .build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    Map<ColumnPath, List<BloomFilter>> inputBloomFilters = allInputBloomFilters();
    Map<ColumnPath, List<BloomFilter>> outputBloomFilters = allOutputBloomFilters(fileDecryptionProperties);
    Set<ColumnPath> schemaRColumns = createSchemaToJoin().getColumns().stream()
        .map(x -> ColumnPath.get(x.getPath()))
        .collect(Collectors.toSet());
    Set<ColumnPath> rBloomFilters = outputBloomFilters.keySet().stream()
        .filter(schemaRColumns::contains)
        .collect(Collectors.toSet());

    // Verify column encryption
    ParquetMetadata metaData = getFileMetaData(outputFile, fileDecryptionProperties);
    assertFalse(metaData.getBlocks().isEmpty());
    List<ColumnChunkMetaData> columns = metaData.getBlocks().get(0).getColumns();
    Set<String> set = ImmutableSet.of(encryptColumn);
    for (ColumnChunkMetaData column : columns) {
      if (set.contains(column.getPath().toDotString())) {
        assertTrue(column.isEncrypted());
      } else {
        assertFalse(column.isEncrypted());
      }
    }

    validateColumnData(
        new HashSet<>(pruneColumns),
        maskColumns.keySet(),
        fileDecryptionProperties,
        joinColumnsOverwrite,
        emptyMap()); // Verify data
    validateSchemaWithGenderColumnPruned(true); // Verify schema
    validateCreatedBy(); // Verify original.created.by
    assertEquals(inputBloomFilters.keySet(), rBloomFilters); // Verify bloom filters
    verifyCodec(outputFile, ImmutableSet.of(CompressionCodecName.ZSTD), fileDecryptionProperties); // Verify codec
    validatePageIndex(ImmutableSet.of(encryptColumn), joinColumnsOverwrite, emptyMap());
  }

  private void testOneInputFileManyInputFilesToJoinSetup() throws IOException {
    inputFiles = Lists.newArrayList(new TestFileBuilder(conf, createSchema())
        .withNumRecord(numRecord)
        .withRowGroupSize(1 * 1024 * 1024)
        .withCodec("GZIP")
        .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
        .withWriterVersion(writerVersion)
        .build());

    List<Long> rowGroupRowCounts = ParquetFileReader.readFooter(
            conf, new Path(inputFiles.get(0).getFileName()), ParquetMetadataConverter.NO_FILTER)
        .getBlocks()
        .stream()
        .map(BlockMetaData::getRowCount)
        .collect(Collectors.toList());

    for (long count : rowGroupRowCounts) {
      inputFilesToJoin.add(new TestFileBuilder(conf, createSchemaToJoin())
          .withNumRecord((int) count)
          .withCodec("UNCOMPRESSED")
          .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
          .withWriterVersion(writerVersion)
          .build());
    }
  }

  private MessageType createSchema() {
    return new MessageType(
        "schema",
        new PrimitiveType(OPTIONAL, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new PrimitiveType(OPTIONAL, BINARY, "Gender"),
        new PrimitiveType(REPEATED, FLOAT, "FloatFraction"),
        new PrimitiveType(OPTIONAL, DOUBLE, "DoubleFraction"),
        new GroupType(
            OPTIONAL,
            "Links",
            new PrimitiveType(REPEATED, BINARY, "Backward"),
            new PrimitiveType(REPEATED, BINARY, "Forward")));
  }

  private MessageType createSchemaToJoin() {
    return new MessageType(
        "schema",
        new PrimitiveType(REPEATED, FLOAT, "FloatFraction"),
        new PrimitiveType(OPTIONAL, INT64, "Age"),
        new GroupType(
            OPTIONAL,
            "Links",
            new PrimitiveType(REPEATED, BINARY, "Backward"),
            new PrimitiveType(REPEATED, BINARY, "Forward")));
  }

  private MessageType createSchemaWithRenamed() {
    return new MessageType(
        "schema",
        new PrimitiveType(OPTIONAL, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "NameRenamed"),
        new PrimitiveType(OPTIONAL, BINARY, "Gender"),
        new PrimitiveType(REPEATED, FLOAT, "FloatFraction"),
        new PrimitiveType(OPTIONAL, DOUBLE, "DoubleFraction"),
        new GroupType(
            OPTIONAL,
            "Links",
            new PrimitiveType(REPEATED, BINARY, "Backward"),
            new PrimitiveType(REPEATED, BINARY, "Forward")));
  }

  private void validateColumnData(
      Set<String> prunePaths,
      Set<String> nullifiedPaths,
      FileDecryptionProperties fileDecryptionProperties,
      Boolean joinColumnsOverwrite,
      Map<String, String> renameColumns)
      throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(outputFile))
        .withConf(conf)
        .withDecryption(fileDecryptionProperties)
        .build();

    List<SimpleGroup> filesMain = inputFiles.stream()
        .flatMap(x -> Arrays.stream(x.getFileContent()))
        .collect(Collectors.toList());
    List<SimpleGroup> filesJoined = inputFilesToJoin.stream()
        .flatMap(x -> Arrays.stream(x.getFileContent()))
        .collect(Collectors.toList());
    BiFunction<String, Integer, Group> groupsExpected = (name, rowIdx) -> {
      if (!filesMain.get(0).getType().containsField(name)
          || joinColumnsOverwrite
              && !filesJoined.isEmpty()
              && filesJoined.get(0).getType().containsField(name)) {
        return filesJoined.get(rowIdx);
      } else {
        return filesMain.get(rowIdx);
      }
    };

    int totalRows =
        inputFiles.stream().mapToInt(x -> x.getFileContent().length).sum();
    for (int i = 0; i < totalRows; i++) {
      Group groupActual = reader.read();
      assertNotNull(groupActual);

      if (!prunePaths.contains("DocId")) {
        if (nullifiedPaths.contains("DocId")) {
          assertThrows(RuntimeException.class, () -> groupActual.getLong("DocId", 0));
        } else {
          assertEquals(
              groupActual.getLong("DocId", 0),
              groupsExpected.apply("DocId", i).getLong("DocId", 0));
        }
      }

      if (!prunePaths.contains("Name") && !nullifiedPaths.contains("Name")) {
        String colName = renameColumns.getOrDefault("Name", "Name");
        assertArrayEquals(
            groupActual.getBinary(colName, 0).getBytes(),
            groupsExpected.apply("Name", i).getBinary("Name", 0).getBytes());
      }

      if (!prunePaths.contains("Gender") && !nullifiedPaths.contains("Gender")) {
        assertArrayEquals(
            groupActual.getBinary("Gender", 0).getBytes(),
            groupsExpected.apply("Gender", i).getBinary("Gender", 0).getBytes());
      }

      if (!prunePaths.contains("FloatFraction") && !nullifiedPaths.contains("FloatFraction")) {
        assertEquals(
            groupActual.getFloat("FloatFraction", 0),
            groupsExpected.apply("FloatFraction", i).getFloat("FloatFraction", 0),
            0);
      }

      if (!prunePaths.contains("DoubleFraction") && !nullifiedPaths.contains("DoubleFraction")) {
        assertEquals(
            groupActual.getDouble("DoubleFraction", 0),
            groupsExpected.apply("DoubleFraction", i).getDouble("DoubleFraction", 0),
            0);
      }

      Group subGroup = groupActual.getGroup("Links", 0);

      if (!prunePaths.contains("Links.Backward") && !nullifiedPaths.contains("Links.Backward")) {
        assertArrayEquals(
            subGroup.getBinary("Backward", 0).getBytes(),
            groupsExpected
                .apply("Links", i)
                .getGroup("Links", 0)
                .getBinary("Backward", 0)
                .getBytes());
      }

      if (!prunePaths.contains("Links.Forward")) {
        if (nullifiedPaths.contains("Links.Forward")) {
          assertThrows(RuntimeException.class, () -> subGroup.getBinary("Forward", 0));
        } else {
          assertArrayEquals(
              subGroup.getBinary("Forward", 0).getBytes(),
              groupsExpected
                  .apply("Links", i)
                  .getGroup("Links", 0)
                  .getBinary("Forward", 0)
                  .getBytes());
        }
      }
    }

    reader.close();
  }

  private ParquetMetadata getFileMetaData(String file, FileDecryptionProperties fileDecryptionProperties)
      throws IOException {
    ParquetReadOptions readOptions = ParquetReadOptions.builder()
        .withDecryption(fileDecryptionProperties)
        .build();
    ParquetMetadata pmd;
    InputFile inputFile = HadoopInputFile.fromPath(new Path(file), conf);
    try (SeekableInputStream in = inputFile.newStream()) {
      pmd = ParquetFileReader.readFooter(inputFile, readOptions, in);
    }
    return pmd;
  }

  private void verifyCodec(
      String file, Set<CompressionCodecName> expectedCodecs, FileDecryptionProperties fileDecryptionProperties)
      throws IOException {
    Set<CompressionCodecName> codecs = new HashSet<>();
    ParquetMetadata pmd = getFileMetaData(file, fileDecryptionProperties);
    for (int i = 0; i < pmd.getBlocks().size(); i++) {
      BlockMetaData block = pmd.getBlocks().get(i);
      for (int j = 0; j < block.getColumns().size(); ++j) {
        ColumnChunkMetaData columnChunkMetaData = block.getColumns().get(j);
        codecs.add(columnChunkMetaData.getCodec());
      }
    }
    assertEquals(expectedCodecs, codecs);
  }

  @FunctionalInterface
  interface CheckedFunction<T, R> {
    R apply(T t) throws IOException;
  }

  private ColumnPath normalizeFieldsInPath(ColumnPath path, Map<String, String> renameColumns) {
    String[] pathArray = path.toArray();
    if (renameColumns != null) {
      pathArray[0] = renameColumns.getOrDefault(pathArray[0], pathArray[0]);
    }
    return ColumnPath.get(pathArray);
  }

  /**
   * Verify the page index is correct.
   *
   * @param exclude the columns to exclude from comparison, for example because they were nullified.
   * @param joinColumnsOverwrite whether a join columns overwrote existing overlapping columns.
   */
  private void validatePageIndex(Set<String> exclude, boolean joinColumnsOverwrite, Map<String, String> renameColumns)
      throws Exception {
    class BlockMeta {
      final TransParquetFileReader reader;
      final BlockMetaData blockMeta;
      final Map<ColumnPath, ColumnChunkMetaData> colPathToMeta;

      BlockMeta(
          TransParquetFileReader reader,
          BlockMetaData blockMeta,
          Map<ColumnPath, ColumnChunkMetaData> colPathToMeta) {
        this.reader = reader;
        this.blockMeta = blockMeta;
        this.colPathToMeta = colPathToMeta;
      }
    }
    CheckedFunction<List<String>, List<BlockMeta>> blockMetaExtractor = files -> {
      List<BlockMeta> result = new ArrayList<>();
      for (String inputFile : files) {
        TransParquetFileReader reader = new TransParquetFileReader(
            HadoopInputFile.fromPath(new Path(inputFile), conf),
            HadoopReadOptions.builder(conf).build());
        reader.getFooter()
            .getBlocks()
            .forEach(blockMetaData -> result.add(new BlockMeta(
                reader,
                blockMetaData,
                blockMetaData.getColumns().stream()
                    .collect(
                        Collectors.toMap(ColumnChunkMetaData::getPath, Function.identity())))));
      }
      return result;
    };

    List<BlockMeta> inBlocksMain = blockMetaExtractor.apply(
        inputFiles.stream().map(EncryptionTestFile::getFileName).collect(Collectors.toList()));
    List<BlockMeta> inBlocksJoined = blockMetaExtractor.apply(
        inputFilesToJoin.stream().map(EncryptionTestFile::getFileName).collect(Collectors.toList()));
    List<BlockMeta> outBlocks = blockMetaExtractor.apply(ImmutableList.of(outputFile));
    Map<String, String> renameColumnsInverted =
        renameColumns.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    for (int blockIdx = 0; blockIdx < outBlocks.size(); blockIdx++) {
      BlockMetaData outBlockMeta = outBlocks.get(blockIdx).blockMeta;
      TransParquetFileReader outReader = outBlocks.get(blockIdx).reader;
      for (ColumnChunkMetaData outChunk : outBlockMeta.getColumns()) {
        if (exclude.contains(outChunk.getPath().toDotString())) continue;
        TransParquetFileReader inReader;
        BlockMetaData inBlockMeta;
        ColumnChunkMetaData inChunk;
        ColumnPath colPath = normalizeFieldsInPath(outChunk.getPath(), renameColumnsInverted);
        if (!inBlocksMain.get(blockIdx).colPathToMeta.containsKey(colPath)
            || joinColumnsOverwrite
                && !inBlocksJoined.isEmpty()
                && inBlocksJoined.get(blockIdx).colPathToMeta.containsKey(colPath)) {
          inReader = inBlocksJoined.get(blockIdx).reader;
          inBlockMeta = inBlocksJoined.get(blockIdx).blockMeta;
          inChunk = inBlocksJoined.get(blockIdx).colPathToMeta.get(colPath);
        } else {
          inReader = inBlocksMain.get(blockIdx).reader;
          inBlockMeta = inBlocksMain.get(blockIdx).blockMeta;
          inChunk = inBlocksMain.get(blockIdx).colPathToMeta.get(colPath);
        }

        ColumnIndex inColumnIndex = inReader.readColumnIndex(inChunk);
        OffsetIndex inOffsetIndex = inReader.readOffsetIndex(inChunk);
        ColumnIndex outColumnIndex = outReader.readColumnIndex(outChunk);
        OffsetIndex outOffsetIndex = outReader.readOffsetIndex(outChunk);
        if (inColumnIndex != null) {
          assertEquals(inColumnIndex.getBoundaryOrder(), outColumnIndex.getBoundaryOrder());
          assertEquals(inColumnIndex.getMaxValues(), outColumnIndex.getMaxValues());
          assertEquals(inColumnIndex.getMinValues(), outColumnIndex.getMinValues());
          assertEquals(inColumnIndex.getNullCounts(), outColumnIndex.getNullCounts());
        }
        if (inOffsetIndex != null) {
          List<Long> inOffsets = getOffsets(inReader, inChunk);
          List<Long> outOffsets = getOffsets(outReader, outChunk);
          assertEquals(inOffsets.size(), outOffsets.size());
          assertEquals(inOffsets.size(), inOffsetIndex.getPageCount());
          assertEquals(inOffsetIndex.getPageCount(), outOffsetIndex.getPageCount());
          for (int k = 0; k < inOffsetIndex.getPageCount(); k++) {
            assertEquals(inOffsetIndex.getFirstRowIndex(k), outOffsetIndex.getFirstRowIndex(k));
            assertEquals(
                inOffsetIndex.getLastRowIndex(k, inBlockMeta.getRowCount()),
                outOffsetIndex.getLastRowIndex(k, outBlockMeta.getRowCount()));
            assertEquals(inOffsetIndex.getOffset(k), (long) inOffsets.get(k));
            assertEquals(outOffsetIndex.getOffset(k), (long) outOffsets.get(k));
          }
        }
      }
    }

    for (BlockMeta t3 : inBlocksMain) t3.reader.close();
    for (BlockMeta t3 : inBlocksJoined) t3.reader.close();
    for (BlockMeta t3 : outBlocks) t3.reader.close();
  }

  private List<Long> getOffsets(TransParquetFileReader reader, ColumnChunkMetaData chunk) throws IOException {
    List<Long> offsets = new ArrayList<>();
    reader.setStreamPosition(chunk.getStartingPos());
    long readValues = 0;
    long totalChunkValues = chunk.getValueCount();
    while (readValues < totalChunkValues) {
      long curOffset = reader.getPos();
      PageHeader pageHeader = reader.readPageHeader();
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          rewriter.readBlock(pageHeader.getCompressed_page_size(), reader);
          break;
        case DATA_PAGE:
          DataPageHeader headerV1 = pageHeader.data_page_header;
          offsets.add(curOffset);
          rewriter.readBlock(pageHeader.getCompressed_page_size(), reader);
          readValues += headerV1.getNum_values();
          break;
        case DATA_PAGE_V2:
          DataPageHeaderV2 headerV2 = pageHeader.data_page_header_v2;
          offsets.add(curOffset);
          int rlLength = headerV2.getRepetition_levels_byte_length();
          rewriter.readBlock(rlLength, reader);
          int dlLength = headerV2.getDefinition_levels_byte_length();
          rewriter.readBlock(dlLength, reader);
          int payLoadLength = pageHeader.getCompressed_page_size() - rlLength - dlLength;
          rewriter.readBlock(payLoadLength, reader);
          readValues += headerV2.getNum_values();
          break;
        default:
          throw new IOException("Not recognized page type");
      }
    }
    return offsets;
  }

  private void validateCreatedBy() throws Exception {
    Set<String> createdBySet = new HashSet<>();
    List<EncryptionTestFile> inFiles =
        Stream.concat(inputFiles.stream(), inputFilesToJoin.stream()).collect(Collectors.toList());
    for (EncryptionTestFile inputFile : inFiles) {
      ParquetMetadata pmd = getFileMetaData(inputFile.getFileName(), null);
      createdBySet.add(pmd.getFileMetaData().getCreatedBy());
      assertNull(pmd.getFileMetaData().getKeyValueMetaData().get(ParquetRewriter.ORIGINAL_CREATED_BY_KEY));
    }

    // Verify created_by from input files have been deduplicated
    Object[] inputCreatedBys = createdBySet.toArray();
    assertEquals(1, inputCreatedBys.length);

    // Verify created_by has been set
    FileMetaData outFMD = getFileMetaData(outputFile, null).getFileMetaData();
    final String createdBy = outFMD.getCreatedBy();
    assertNotNull(createdBy);
    assertEquals(createdBy, Version.FULL_VERSION);

    // Verify original.created.by has been set
    String inputCreatedBy = (String) inputCreatedBys[0];
    String originalCreatedBy = outFMD.getKeyValueMetaData().get(ParquetRewriter.ORIGINAL_CREATED_BY_KEY);
    assertEquals(inputCreatedBy, originalCreatedBy);
  }

  private void validateRowGroupRowCount() throws Exception {
    List<Long> inputRowCounts = new ArrayList<>();
    for (EncryptionTestFile inputFile : inputFiles) {
      ParquetMetadata inputPmd = getFileMetaData(inputFile.getFileName(), null);
      for (BlockMetaData blockMetaData : inputPmd.getBlocks()) {
        inputRowCounts.add(blockMetaData.getRowCount());
      }
    }

    List<Long> outputRowCounts = new ArrayList<>();
    ParquetMetadata outPmd = getFileMetaData(outputFile, null);
    for (BlockMetaData blockMetaData : outPmd.getBlocks()) {
      outputRowCounts.add(blockMetaData.getRowCount());
    }

    assertEquals(inputRowCounts, outputRowCounts);
  }

  private Map<ColumnPath, List<BloomFilter>> allInputBloomFilters() throws Exception {
    Map<ColumnPath, List<BloomFilter>> inputBloomFilters = new HashMap<>();
    List<EncryptionTestFile> files =
        Stream.concat(inputFiles.stream(), inputFilesToJoin.stream()).collect(Collectors.toList());
    for (EncryptionTestFile inputFile : files) {
      Map<ColumnPath, List<BloomFilter>> bloomFilters = allBloomFilters(inputFile.getFileName(), null);
      for (Map.Entry<ColumnPath, List<BloomFilter>> entry : bloomFilters.entrySet()) {
        List<BloomFilter> bloomFilterList = inputBloomFilters.getOrDefault(entry.getKey(), new ArrayList<>());
        bloomFilterList.addAll(entry.getValue());
        inputBloomFilters.put(entry.getKey(), bloomFilterList);
      }
    }

    return inputBloomFilters;
  }

  private Map<ColumnPath, List<BloomFilter>> allOutputBloomFilters(FileDecryptionProperties fileDecryptionProperties)
      throws Exception {
    return allBloomFilters(outputFile, fileDecryptionProperties);
  }

  private Map<ColumnPath, List<BloomFilter>> allBloomFilters(
      String path, FileDecryptionProperties fileDecryptionProperties) throws Exception {
    Map<ColumnPath, List<BloomFilter>> allBloomFilters = new HashMap<>();
    ParquetReadOptions readOptions = ParquetReadOptions.builder()
        .withDecryption(fileDecryptionProperties)
        .build();
    InputFile inputFile = HadoopInputFile.fromPath(new Path(path), conf);
    try (TransParquetFileReader reader = new TransParquetFileReader(inputFile, readOptions)) {
      ParquetMetadata metadata = reader.getFooter();
      for (BlockMetaData blockMetaData : metadata.getBlocks()) {
        for (ColumnChunkMetaData columnChunkMetaData : blockMetaData.getColumns()) {
          BloomFilter bloomFilter = reader.readBloomFilter(columnChunkMetaData);
          if (bloomFilter != null) {
            List<BloomFilter> bloomFilterList =
                allBloomFilters.getOrDefault(columnChunkMetaData.getPath(), new ArrayList<>());
            bloomFilterList.add(bloomFilter);
            allBloomFilters.put(columnChunkMetaData.getPath(), bloomFilterList);
          }
        }
      }
    }

    return allBloomFilters;
  }

  private RewriteOptions.Builder createBuilder(List<Path> inputPaths) throws IOException {
    return createBuilder(inputPaths, new ArrayList<>(), false);
  }

  private RewriteOptions.Builder createBuilder(
      List<Path> inputPathsL, List<Path> inputPathsR, boolean overwriteInputWithJoinColumns) throws IOException {
    RewriteOptions.Builder builder;
    if (usingHadoop) {
      Path outputPath = new Path(outputFile);
      builder = new RewriteOptions.Builder(conf, inputPathsL, inputPathsR, outputPath);
    } else {
      OutputFile outputPath = HadoopOutputFile.fromPath(new Path(outputFile), conf);
      List<InputFile> inputsL = inputPathsL.stream()
          .map(p -> HadoopInputFile.fromPathUnchecked(p, conf))
          .collect(Collectors.toList());
      List<InputFile> inputsR = inputPathsR.stream()
          .map(p -> HadoopInputFile.fromPathUnchecked(p, conf))
          .collect(Collectors.toList());
      builder = new RewriteOptions.Builder(parquetConf, inputsL, inputsR, outputPath);
    }
    builder.overwriteInputWithJoinColumns(overwriteInputWithJoinColumns);
    return builder;
  }

  private void validateSchemaWithGenderColumnPruned(boolean addJoinedColumn) throws IOException {
    MessageType expectSchema = new MessageType(
        "schema",
        new PrimitiveType(OPTIONAL, INT64, "DocId"),
        new PrimitiveType(REQUIRED, BINARY, "Name"),
        new PrimitiveType(REPEATED, FLOAT, "FloatFraction"),
        new PrimitiveType(OPTIONAL, DOUBLE, "DoubleFraction"),
        new GroupType(
            OPTIONAL,
            "Links",
            new PrimitiveType(REPEATED, BINARY, "Backward"),
            new PrimitiveType(REPEATED, BINARY, "Forward")));
    if (addJoinedColumn) {
      expectSchema = expectSchema.union(new MessageType("schema", new PrimitiveType(OPTIONAL, INT64, "Age")));
    }
    MessageType actualSchema = ParquetFileReader.readFooter(
            conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER)
        .getFileMetaData()
        .getSchema();
    assertEquals(expectSchema, actualSchema);
  }

  private void addGzipInputFile() {
    if (!inputFiles.contains(gzipEncryptionTestFileWithoutBloomFilterColumn)) {
      inputFiles.add(this.gzipEncryptionTestFileWithoutBloomFilterColumn);
    }
  }

  private void addUncompressedInputFile() {
    if (!inputFiles.contains(uncompressedEncryptionTestFileWithoutBloomFilterColumn)) {
      inputFiles.add(uncompressedEncryptionTestFileWithoutBloomFilterColumn);
    }
  }
}
