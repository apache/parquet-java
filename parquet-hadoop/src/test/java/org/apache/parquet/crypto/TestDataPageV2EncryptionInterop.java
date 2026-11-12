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

package org.apache.parquet.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.crypto.keytools.KeyToolkit;
import org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory;
import org.apache.parquet.crypto.keytools.mocks.Base64KmsClient;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.FileCryptoMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.rewrite.ParquetRewriter;
import org.apache.parquet.hadoop.rewrite.RewriteOptions;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Encryption of DataPageV2 pages with repetition or definition levels (apache/parquet-java#3934).
 *
 * <p>Fixtures in {@code src/test/resources/encryption/datapagev2}:
 *
 * <ul>
 *   <li>{@code arrow-v2-*}: written by Arrow C++ (pyarrow 24.0.0).
 *   <li>{@code java-legacy-v2-*}: written with {@link #writeWithParquetJava} at commit e265ac7f5,
 *       before the levels were encrypted. To regenerate, check out that commit and run it for each
 *       cipher and codec.
 *   <li>{@code legacy-v2-ambiguous-*}: one hand-built legacy page, see {@link
 *       #readsALegacyPageThatLooksLikeASingleModule}. Written with a deprecated {@code
 *       ParquetFileWriter.writeDataPageV2} overload that takes only a page header encryptor.
 * </ul>
 */
public class TestDataPageV2EncryptionInterop {

  private static final String FIXTURES = "/encryption/datapagev2/";

  private static final int NUM_ROWS = 500;

  /** Footer key, also the key of all columns with uniform encryption. */
  private static final byte[] FOOTER_KEY = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);

  private static final byte[] FOOTER_KEY_METADATA = "footkey".getBytes(StandardCharsets.UTF_8);

  private static final byte[] COLUMN_KEY = "fedcba9876543210".getBytes(StandardCharsets.UTF_8);

  private static final byte[] COLUMN_KEY_METADATA = "colkey".getBytes(StandardCharsets.UTF_8);

  /**
   * {@code seq} is required (no levels), {@code id} and {@code name} are optional (definition
   * levels), {@code tags} is a list (repetition and definition levels).
   */
  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("message schema {"
      + "  required int64 seq;"
      + "  optional int32 id;"
      + "  optional binary name (STRING);"
      + "  optional group tags (LIST) {"
      + "    repeated group list {"
      + "      optional binary element (STRING);"
      + "    }"
      + "  }"
      + "}");

  /** {@code seq >= 250} lets the first two pages (rows 0-199) be skipped. */
  private static final int FIRST_ROW_OF_PAGE_WITH_SEQ_250 = 200;

  private static final FilterCompat.Filter SEQ_AT_LEAST_250 =
      FilterCompat.get(FilterApi.gtEq(FilterApi.longColumn("seq"), 250L));

  @TempDir
  private java.nio.file.Path tempDir;

  @BeforeEach
  @AfterEach
  public void clearKmsClients() {
    // KMS clients are cached per JVM by instance id, and the Arrow fixtures use the default id
    KeyToolkit.removeCacheEntriesForAllTokens();
  }

  static Stream<Arguments> fixturesAndMemoryModes(String prefix) {
    List<Arguments> arguments = new ArrayList<>();
    for (String cipher : new String[] {"aes-gcm-v1", "aes-gcm-ctr-v1"}) {
      for (String codec : new String[] {"uncompressed", "snappy"}) {
        for (boolean directMemory : new boolean[] {false, true}) {
          arguments.add(Arguments.of(prefix + cipher + "-" + codec + ".parquet", directMemory));
        }
      }
    }
    return arguments.stream();
  }

  static Stream<Arguments> arrowFixtures() {
    return fixturesAndMemoryModes("arrow-v2-");
  }

  static Stream<Arguments> legacyFixtures() {
    return fixturesAndMemoryModes("java-legacy-v2-");
  }

  static Stream<String> legacyFixtureNames() {
    return Stream.of("aes-gcm-v1", "aes-gcm-ctr-v1")
        .flatMap(cipher -> Stream.of("uncompressed", "snappy")
            .map(codec -> "java-legacy-v2-" + cipher + "-" + codec + ".parquet"));
  }

  static Stream<Arguments> ambiguousFixtures() {
    return Stream.of("aes-gcm-v1", "aes-gcm-ctr-v1")
        .flatMap(cipher -> Stream.of(false, true)
            .map(directMemory -> Arguments.of("legacy-v2-ambiguous-" + cipher + ".parquet", directMemory)));
  }

  static Stream<Arguments> ciphersAndCodecs() {
    List<Arguments> arguments = new ArrayList<>();
    for (ParquetCipher cipher : ParquetCipher.values()) {
      for (CompressionCodecName codec :
          new CompressionCodecName[] {CompressionCodecName.UNCOMPRESSED, CompressionCodecName.SNAPPY}) {
        arguments.add(Arguments.of(cipher, codec));
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "{0}, directMemory={1}")
  @MethodSource("arrowFixtures")
  public void readsFilesWrittenByArrow(String fixture, boolean directMemory) throws Exception {
    Configuration conf = new Configuration();
    conf.set(
        EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
        PropertiesDrivenCryptoFactory.class.getName());
    conf.set(KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME, Base64KmsClient.class.getName());

    assertThat(readRows(fixture(fixture), conf, null, directMemory)).containsExactlyElementsOf(expectedRows());
  }

  @ParameterizedTest(name = "{0}, directMemory={1}")
  @MethodSource("legacyFixtures")
  public void readsFilesWrittenByEarlierParquetJavaVersions(String fixture, boolean directMemory) throws Exception {
    assertThat(readRows(fixture(fixture), new Configuration(), decryptionProperties(), directMemory))
        .containsExactlyElementsOf(expectedRows());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("legacyFixtureNames")
  public void legacyFixturesHaveThePlaintextLevelsLayout(String fixture) throws Exception {
    List<DecodedPage> pages = decodeDataPagesV2(fixture(fixture), FOOTER_KEY, false);

    assertThat(pages).isNotEmpty();
    assertThat(pages).allSatisfy(page -> assertThat(page.singleModule).isFalse());
  }

  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("ciphersAndCodecs")
  public void writesTheLevelsInTheEncryptedPageModule(ParquetCipher cipher, CompressionCodecName codec)
      throws Exception {
    Path file = writeWithParquetJava(cipher, codec);

    List<DecodedPage> pages = decodeDataPagesV2(file, FOOTER_KEY, false);

    assertThat(pages).isNotEmpty();
    assertThat(pages).extracting(page -> page.column).containsOnly("id", "name", "tags.list.element");
    assertThat(pages)
        .anySatisfy(page -> assertThat(page.repetitionLevelsLength).isPositive());
    for (DecodedPage page : pages) {
      assertThat(page.singleModule)
          .as("the levels of %s must be inside the encrypted page module", page)
          .isTrue();
      assertThat(page.decryptedLength).isGreaterThanOrEqualTo(page.levelsLength());
      if (codec == CompressionCodecName.UNCOMPRESSED) {
        assertThat(page.decryptedLength).isEqualTo(page.uncompressedPageSize);
      }
    }
  }

  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("ciphersAndCodecs")
  public void readsWhatItWrites(ParquetCipher cipher, CompressionCodecName codec) throws Exception {
    Path file = writeWithParquetJava(cipher, codec);

    assertThat(readRows(file, new Configuration(), decryptionProperties(), false))
        .containsExactlyElementsOf(expectedRows());
    assertThat(readRows(file, new Configuration(), decryptionProperties(), true))
        .containsExactlyElementsOf(expectedRows());
  }

  @Test
  public void detectsTamperingWithTheLevels() throws Exception {
    Path file = writeFileWithTamperedLevels();
    // Otherwise the page checksum fails first
    Configuration withoutChecksums = new Configuration();
    withoutChecksums.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false);

    assertThatThrownBy(() -> readRows(file, withoutChecksums, decryptionProperties(), false))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Can not read value at 0 in block -1")
        .cause()
        .isInstanceOf(TagVerificationException.class)
        .hasMessage("GCM tag check failed");
  }

  @Test
  public void pageChecksumDetectsTamperingWithTheLevels() throws Exception {
    Path file = writeFileWithTamperedLevels();

    assertThatThrownBy(() -> readRows(file, new Configuration(), decryptionProperties(), false))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Can not read value at 0 in block -1")
        .cause()
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("could not verify page integrity, CRC checksum verification failed");
  }

  /** Writes a file with one bit of the levels of the first DataPageV2 page flipped. */
  private Path writeFileWithTamperedLevels() throws Exception {
    Path file = writeWithParquetJava(ParquetCipher.AES_GCM_V1, CompressionCodecName.UNCOMPRESSED);
    DecodedPage page = decodeDataPagesV2(file, FOOTER_KEY, false).get(0);

    // First ciphertext byte, which is the first byte of the levels
    File source = new File(file.toUri());
    try (RandomAccessFile raf = new RandomAccessFile(source, "rw")) {
      long position = page.pageBytesOffset + Integer.BYTES + AesCipher.NONCE_LENGTH;
      raf.seek(position);
      int original = raf.read();
      raf.seek(position);
      raf.write(original ^ 0x01);
    }
    // Otherwise the local filesystem rejects the file
    Files.deleteIfExists(new File(source.getParent(), "." + source.getName() + ".crc").toPath());
    return file;
  }

  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("ciphersAndCodecs")
  public void rewriterWritesTheLevelsInTheEncryptedPageModule(ParquetCipher cipher, CompressionCodecName codec)
      throws Exception {
    Path plaintext = writeWithParquetJava(null, codec);
    Path rewritten = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());

    String[] encryptColumns = {"id", "name", "tags.list.element"};
    Map<ColumnPath, ColumnEncryptionProperties> columns = new HashMap<>();
    for (String column : encryptColumns) {
      ColumnPath path = ColumnPath.fromDotString(column);
      columns.put(
          path,
          ColumnEncryptionProperties.builder(path)
              .withKey(COLUMN_KEY)
              .withKeyMetaData(COLUMN_KEY_METADATA)
              .build());
    }
    FileEncryptionProperties encryption = FileEncryptionProperties.builder(FOOTER_KEY)
        .withFooterKeyMetadata(FOOTER_KEY_METADATA)
        .withAlgorithm(cipher)
        .withEncryptedColumns(columns)
        .build();
    RewriteOptions options = new RewriteOptions.Builder(new Configuration(), plaintext, rewritten)
        .encrypt(Arrays.asList(encryptColumns))
        .encryptionProperties(encryption)
        .build();
    try (ParquetRewriter rewriter = new ParquetRewriter(options)) {
      rewriter.processBlocks();
    }

    List<DecodedPage> pages = decodeDataPagesV2(rewritten, COLUMN_KEY, true);
    assertThat(pages).isNotEmpty();
    assertThat(pages).extracting(page -> page.column).containsOnly("id", "name", "tags.list.element");
    assertThat(pages)
        .anySatisfy(page -> assertThat(page.repetitionLevelsLength).isPositive());
    assertThat(pages).allSatisfy(page -> assertThat(page.singleModule).isTrue());
    assertThat(readRows(rewritten, new Configuration(), decryptionProperties(), false))
        .containsExactlyElementsOf(expectedRows());
    assertThat(readRows(rewritten, new Configuration(), decryptionProperties(), true))
        .containsExactlyElementsOf(expectedRows());
  }

  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("ciphersAndCodecs")
  public void readsWhatItWritesInSeveralRowGroups(ParquetCipher cipher, CompressionCodecName codec) throws Exception {
    Path file = writeWithParquetJava(cipher, codec, 4096);

    try (ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(file, new Configuration()),
        ParquetReadOptions.builder()
            .withDecryption(decryptionProperties())
            .build())) {
      assertThat(reader.getFooter().getBlocks()).hasSizeGreaterThan(1);
    }
    assertThat(decodeDataPagesV2(file, FOOTER_KEY, false))
        .allSatisfy(page -> assertThat(page.singleModule).isTrue());
    assertThat(readRows(file, new Configuration(), decryptionProperties(), false))
        .containsExactlyElementsOf(expectedRows());
    assertThat(readRows(file, new Configuration(), decryptionProperties(), true))
        .containsExactlyElementsOf(expectedRows());
  }

  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("ciphersAndCodecs")
  public void skipsPagesOfWhatItWrites(ParquetCipher cipher, CompressionCodecName codec) throws Exception {
    Path file = writeWithParquetJava(cipher, codec);

    assertThat(readRows(file, new Configuration(), decryptionProperties(), false, SEQ_AT_LEAST_250))
        .containsExactlyElementsOf(expectedRows().subList(FIRST_ROW_OF_PAGE_WITH_SEQ_250, NUM_ROWS));
    assertThat(readRows(file, new Configuration(), decryptionProperties(), true, SEQ_AT_LEAST_250))
        .containsExactlyElementsOf(expectedRows().subList(FIRST_ROW_OF_PAGE_WITH_SEQ_250, NUM_ROWS));
  }

  @ParameterizedTest(name = "{0}, directMemory={1}")
  @MethodSource("legacyFixtures")
  public void skipsPagesOfFilesWrittenByEarlierParquetJavaVersions(String fixture, boolean directMemory)
      throws Exception {
    assertThat(readRows(
            fixture(fixture), new Configuration(), decryptionProperties(), directMemory, SEQ_AT_LEAST_250))
        .containsExactlyElementsOf(expectedRows().subList(FIRST_ROW_OF_PAGE_WITH_SEQ_250, NUM_ROWS));
  }

  /**
   * {@code legacy-v2-ambiguous-*}: one legacy page of 8 optional int32 values, only the first (12345)
   * defined. Its 4 bytes of definition levels, {@code 03 01 00 00}, are a bit-packed run of 8 levels
   * plus 2 unread bytes, and read as an LE int they equal {@code compressed_page_size - 4}, so the
   * page fits both layouts. The {@code created_by} of the fixture, {@code parquet-mr version 1.19.0-SNAPSHOT}, resolves
   * it to plaintext levels.
   */
  @ParameterizedTest(name = "{0}, directMemory={1}")
  @MethodSource("ambiguousFixtures")
  public void readsALegacyPageThatLooksLikeASingleModule(String fixture, boolean directMemory) throws Exception {
    List<String> rows = readRows(fixture(fixture), new Configuration(), decryptionProperties(), directMemory);

    assertThat(rows).hasSize(8);
    assertThat(rows.get(0)).contains("v: 12345");
    assertThat(rows.subList(1, 8)).allSatisfy(row -> assertThat(row).doesNotContain("v:"));
  }

  @ParameterizedTest(name = "{0}, directMemory={1}")
  @MethodSource("ambiguousFixtures")
  public void readsAnAmbiguousPageWithPlaintextLevelsIfTheFileIsFromParquetJava118(
      String fixture, boolean directMemory) throws Exception {
    List<String> rows = readRowsWrittenBy(fixture(fixture), "parquet-mr version 1.18.0 (build abc)", directMemory);

    assertThat(rows).hasSize(8);
    assertThat(rows.get(0)).contains("v: 12345");
    assertThat(rows.subList(1, 8)).allSatisfy(row -> assertThat(row).doesNotContain("v:"));
  }

  @ParameterizedTest(name = "directMemory={0}")
  @ValueSource(booleans = {false, true})
  public void readsAnAmbiguousPageAsASingleModuleIfTheFileIsFromAnotherWriter(boolean directMemory) {
    // The fixture has plaintext levels, so reading it as a single module fails the GCM tag check
    assertThatThrownBy(() -> readRowsWrittenBy(
            fixture("legacy-v2-ambiguous-aes-gcm-v1.parquet"),
            "parquet-cpp-arrow version 24.0.0",
            directMemory))
        .isInstanceOf(TagVerificationException.class)
        .hasMessage("GCM tag check failed");
  }

  @Test
  public void writerRejectsAnEncryptedPageHeaderWithoutAnEncryptedPage() throws Exception {
    Path file = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
    try (ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(file, new Configuration()),
        SCHEMA,
        ParquetFileWriter.Mode.CREATE,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.MAX_PADDING_SIZE_DEFAULT,
        null,
        ParquetProperties.builder().build())) {
      BlockCipher.Encryptor headerEncryptor = ModuleCipherFactory.getEncryptor(AesMode.GCM, FOOTER_KEY);

      assertThatThrownBy(() -> writer.writeDataPageV2(
              1,
              0,
              1,
              BytesInput.empty(),
              BytesInput.from(new byte[] {0x02, 0x01}),
              Encoding.PLAIN,
              BytesInput.from(new byte[4]),
              false,
              4,
              Statistics.createStats(SCHEMA.getType("id").asPrimitiveType()),
              headerEncryptor,
              new byte[0],
              null,
              null,
              null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Both the page header and the page of an encrypted column must be encrypted");
    }
  }

  /** Writes 100 rows per page; encrypts all columns with the footer key if {@code cipher} is set. */
  private Path writeWithParquetJava(ParquetCipher cipher, CompressionCodecName codec) throws IOException {
    return writeWithParquetJava(cipher, codec, ParquetWriter.DEFAULT_BLOCK_SIZE);
  }

  private Path writeWithParquetJava(ParquetCipher cipher, CompressionCodecName codec, long rowGroupSize)
      throws IOException {
    Path file = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(file)
        .withType(SCHEMA)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withCompressionCodec(codec)
        .withRowGroupSize(rowGroupSize)
        .withMinRowCountForPageSizeCheck(10)
        .withPageRowCountLimit(100);
    if (cipher != null) {
      builder.withEncryption(FileEncryptionProperties.builder(FOOTER_KEY)
          .withFooterKeyMetadata(FOOTER_KEY_METADATA)
          .withAlgorithm(cipher)
          .build());
    }
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < NUM_ROWS; i++) {
        writer.write(row(i));
      }
    }
    return file;
  }

  /** Builds row {@code i}, as stored in the {@code arrow-v2-*} fixtures. */
  private static Group row(int i) {
    Group group = new SimpleGroupFactory(SCHEMA).newGroup();
    group.add("seq", (long) i);
    if (i % 7 != 0) {
      group.add("id", i);
    }
    if (i % 5 != 0) {
      group.add("name", "name-" + i);
    }
    if (i % 4 != 0) {
      Group tags = group.addGroup("tags");
      int count = i % 4;
      for (int j = 0; j < count; j++) {
        Group element = tags.addGroup("list");
        if ((i + j) % 3 != 0) {
          element.add("element", "tag-" + i + "-" + j);
        }
      }
    }
    return group;
  }

  private static List<String> expectedRows() {
    List<String> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      rows.add(row(i).toString());
    }
    return rows;
  }

  private static FileDecryptionProperties decryptionProperties() {
    return FileDecryptionProperties.builder()
        .withKeyRetriever(new DecryptionKeyRetrieverMock()
            .putKey("footkey", FOOTER_KEY)
            .putKey("colkey", COLUMN_KEY))
        .build();
  }

  /**
   * Reads all rows as strings. Page checksums are verified unless {@code conf} disables it.
   *
   * @param conf not modified
   * @param decryption the decryption properties, or null to use the ones configured in {@code conf}
   * @param directMemory whether to read into direct buffers and decrypt off heap
   */
  private static List<String> readRows(
      Path file, Configuration conf, FileDecryptionProperties decryption, boolean directMemory)
      throws IOException {
    return readRows(file, conf, decryption, directMemory, FilterCompat.NOOP);
  }

  /** Like {@link #readRows(Path, Configuration, FileDecryptionProperties, boolean)}, filtered by page. */
  private static List<String> readRows(
      Path file,
      Configuration conf,
      FileDecryptionProperties decryption,
      boolean directMemory,
      FilterCompat.Filter filter)
      throws IOException {
    Configuration readConf = new Configuration(conf);
    readConf.setBoolean(ParquetInputFormat.OFF_HEAP_DECRYPT_BUFFER_ENABLED, directMemory);
    readConf.setBoolean(
        ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED,
        conf.getBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true));
    if (filter != FilterCompat.NOOP) {
      // Return every row of the pages that are read, so the test can see which pages were skipped
      readConf.setBoolean(ParquetInputFormat.RECORD_FILTERING_ENABLED, false);
    }
    List<String> rows = new ArrayList<>();
    // close() fails on leaked buffers
    try (TrackingByteBufferAllocator allocator = TrackingByteBufferAllocator.wrap(
        directMemory ? new DirectByteBufferAllocator() : new HeapByteBufferAllocator())) {
      ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), file)
          .withConf(readConf)
          .withFilter(filter)
          .withAllocator(allocator);
      if (decryption != null) {
        builder.withDecryption(decryption);
      }
      try (ParquetReader<Group> reader = builder.build()) {
        Group group;
        while ((group = reader.read()) != null) {
          rows.add(group.toString());
        }
      }
    }
    return rows;
  }

  /** Reads all rows as strings, with {@code createdBy} in place of the {@code created_by} of the file. */
  private static List<String> readRowsWrittenBy(Path file, String createdBy, boolean directMemory)
      throws IOException {
    Configuration conf = new Configuration();
    InputFile inputFile = HadoopInputFile.fromPath(file, conf);
    List<String> rows = new ArrayList<>();
    // close() fails on leaked buffers
    try (TrackingByteBufferAllocator allocator = TrackingByteBufferAllocator.wrap(
        directMemory ? new DirectByteBufferAllocator() : new HeapByteBufferAllocator())) {
      ParquetReadOptions options = HadoopReadOptions.builder(conf, file)
          .withDecryption(decryptionProperties())
          .withAllocator(allocator)
          .useOffHeapDecryptBuffer(directMemory)
          .build();
      ParquetMetadata footer;
      try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options)) {
        footer = reader.getFooter();
      }
      FileMetaData fileMetaData = footer.getFileMetaData();
      ParquetMetadata writtenBy = new ParquetMetadata(
          new FileMetaData(
              fileMetaData.getSchema(),
              fileMetaData.getKeyValueMetaData(),
              createdBy,
              fileMetaData.getEncryptionType(),
              fileMetaData.getFileDecryptor()),
          footer.getBlocks());
      MessageType schema = fileMetaData.getSchema();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      try (ParquetFileReader reader =
          new ParquetFileReader(inputFile, writtenBy, options, inputFile.newStream())) {
        PageReadStore rowGroup;
        while ((rowGroup = reader.readNextRowGroup()) != null) {
          RecordReader<Group> records = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
          for (long i = 0; i < rowGroup.getRowCount(); i++) {
            rows.add(records.read().toString());
          }
          rowGroup.close();
        }
      }
    }
    return rows;
  }

  private static Path fixture(String name) throws URISyntaxException {
    return new Path(TestDataPageV2EncryptionInterop.class
        .getResource(FIXTURES + name)
        .toURI());
  }

  /** A DataPageV2 with levels, as found by {@link #decodeDataPagesV2}. */
  private static final class DecodedPage {
    private final String column;
    private final int repetitionLevelsLength;
    private final int definitionLevelsLength;
    private final long pageBytesOffset;
    private final boolean singleModule;
    private final int decryptedLength;
    private final int uncompressedPageSize;

    private DecodedPage(
        String column,
        int repetitionLevelsLength,
        int definitionLevelsLength,
        long pageBytesOffset,
        boolean singleModule,
        int decryptedLength,
        int uncompressedPageSize) {
      this.column = column;
      this.repetitionLevelsLength = repetitionLevelsLength;
      this.definitionLevelsLength = definitionLevelsLength;
      this.pageBytesOffset = pageBytesOffset;
      this.singleModule = singleModule;
      this.decryptedLength = decryptedLength;
      this.uncompressedPageSize = uncompressedPageSize;
    }

    private int levelsLength() {
      return repetitionLevelsLength + definitionLevelsLength;
    }

    @Override
    public String toString() {
      return "DataPageV2 of column " + column + " at offset " + pageBytesOffset + " with " + levelsLength()
          + " bytes of levels";
    }
  }

  /**
   * Decodes the DataPageV2 pages of a file without the parquet-java page reader: parses and decrypts
   * the page headers, then decrypts the bytes after each header as one module if their first 4 bytes
   * are {@code size - 4}, otherwise the bytes after the levels.
   *
   * @param file file with an encrypted footer, whose encrypted columns all use {@code columnKey}
   * @param onlyEncryptedColumns whether to skip columns that are not encrypted
   * @return the pages that have levels, in file order
   */
  private static List<DecodedPage> decodeDataPagesV2(Path file, byte[] columnKey, boolean onlyEncryptedColumns)
      throws IOException {
    byte[] bytes = Files.readAllBytes(new File(file.toUri()).toPath());
    int footerLength = BytesUtils.readIntLittleEndian(bytes, bytes.length - 8);
    FileCryptoMetaData cryptoMetaData = Util.readFileCryptoMetaData(
        new ByteArrayInputStream(bytes, bytes.length - 8 - footerLength, footerLength));
    org.apache.parquet.format.EncryptionAlgorithm algorithm = cryptoMetaData.getEncryption_algorithm();
    boolean ctr = algorithm.isSetAES_GCM_CTR_V1();
    byte[] fileAAD = (ctr
        ? algorithm.getAES_GCM_CTR_V1().getAad_file_unique()
        : algorithm.getAES_GCM_V1().getAad_file_unique());

    BlockCipher.Decryptor headerDecryptor = ModuleCipherFactory.getDecryptor(AesMode.GCM, columnKey);
    BlockCipher.Decryptor pageDecryptor =
        ModuleCipherFactory.getDecryptor(ctr ? AesMode.CTR : AesMode.GCM, columnKey);

    List<DecodedPage> decoded = new ArrayList<>();
    try (ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(file, new Configuration()),
        ParquetReadOptions.builder()
            .withDecryption(decryptionProperties())
            .build())) {
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (int columnOrdinal = 0; columnOrdinal < block.getColumns().size(); columnOrdinal++) {
          ColumnChunkMetaData chunk = block.getColumns().get(columnOrdinal);
          if (onlyEncryptedColumns && !chunk.isEncrypted()) {
            continue;
          }
          ByteArrayInputStream in = new ByteArrayInputStream(bytes);
          in.skip(chunk.getStartingPos());
          boolean dictionaryPageRead = !chunk.hasDictionaryPage();
          int pageOrdinal = 0;
          long valuesRead = 0;
          while (valuesRead < chunk.getValueCount()) {
            byte[] headerAAD = dictionaryPageRead
                ? AesCipher.createModuleAAD(
                    fileAAD,
                    ModuleType.DataPageHeader,
                    block.getOrdinal(),
                    columnOrdinal,
                    pageOrdinal)
                : AesCipher.createModuleAAD(
                    fileAAD,
                    ModuleType.DictionaryPageHeader,
                    block.getOrdinal(),
                    columnOrdinal,
                    -1);
            PageHeader header = Util.readPageHeader(in, headerDecryptor, headerAAD);
            long pageBytesOffset = bytes.length - in.available();
            byte[] pageBytes = in.readNBytes(header.getCompressed_page_size());
            assertThat(pageBytes).hasSize(header.getCompressed_page_size());
            if (!dictionaryPageRead) {
              dictionaryPageRead = true;
              continue;
            }
            if (header.getType() == PageType.DATA_PAGE_V2) {
              int repetitionLevels =
                  header.getData_page_header_v2().getRepetition_levels_byte_length();
              int definitionLevels =
                  header.getData_page_header_v2().getDefinition_levels_byte_length();
              int levels = repetitionLevels + definitionLevels;
              boolean singleModule =
                  BytesUtils.readIntLittleEndian(pageBytes, 0) == pageBytes.length - Integer.BYTES;
              byte[] module =
                  singleModule ? pageBytes : Arrays.copyOfRange(pageBytes, levels, pageBytes.length);
              assertThat(BytesUtils.readIntLittleEndian(module, 0))
                  .isEqualTo(module.length - Integer.BYTES);
              byte[] pageAAD = AesCipher.createModuleAAD(
                  fileAAD, ModuleType.DataPage, block.getOrdinal(), columnOrdinal, pageOrdinal);
              byte[] decrypted = pageDecryptor.decrypt(module, pageAAD);
              if (levels > 0) {
                decoded.add(new DecodedPage(
                    chunk.getPath().toDotString(),
                    repetitionLevels,
                    definitionLevels,
                    pageBytesOffset,
                    singleModule,
                    decrypted.length,
                    header.getUncompressed_page_size()));
              }
              valuesRead += header.getData_page_header_v2().getNum_values();
            }
            pageOrdinal++;
          }
        }
      }
    }
    return decoded;
  }
}
