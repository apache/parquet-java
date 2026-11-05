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

import static org.apache.parquet.crypto.DataPageV2EncryptionTestData.COLUMN_KEY;
import static org.apache.parquet.crypto.DataPageV2EncryptionTestData.COLUMN_KEY_METADATA;
import static org.apache.parquet.crypto.DataPageV2EncryptionTestData.FOOTER_KEY;
import static org.apache.parquet.crypto.DataPageV2EncryptionTestData.FOOTER_KEY_METADATA;
import static org.apache.parquet.crypto.DataPageV2EncryptionTestData.NUM_ROWS;
import static org.apache.parquet.crypto.DataPageV2EncryptionTestData.SCHEMA;
import static org.apache.parquet.crypto.DataPageV2EncryptionTestData.decryptionProperties;
import static org.apache.parquet.crypto.DataPageV2EncryptionTestData.expectedRows;
import static org.apache.parquet.crypto.DataPageV2EncryptionTestData.readRows;
import static org.apache.parquet.crypto.DataPageV2EncryptionTestData.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
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
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.crypto.keytools.KeyToolkit;
import org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.FileCryptoMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.rewrite.ParquetRewriter;
import org.apache.parquet.hadoop.rewrite.RewriteOptions;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Interoperability of the encryption of DataPageV2 pages that have repetition or definition levels.
 *
 * <p>The page body of such a page, the levels included, is a single encrypted "data page" module.
 * Arrow C++ and arrow-rs write it that way, and so does parquet-java. Earlier parquet-java versions
 * left the levels in plaintext and encrypted only the values (apache/parquet-java#3934), so files
 * in both layouts exist. These tests make sure that
 *
 * <ul>
 *   <li>parquet-java writes only the single module layout, which an independent decoder written
 *       from the specification confirms, also when it rewrites files;
 *   <li>parquet-java reads the files that Arrow wrote, which the {@code arrow-v2-*} fixtures are;
 *   <li>parquet-java still reads the files that it wrote in the old layout, which the {@code
 *       java-legacy-v2-*} fixtures are. They were written by parquet-java 1.19.0-SNAPSHOT at commit
 *       e265ac7f5, just before the layout changed, with the footer key as the key of all columns,
 *       the same rows as {@link DataPageV2EncryptionTestData#row(int)}, 100 rows per page and
 *       {@code ExampleParquetWriter} with {@code WriterVersion.PARQUET_2_0}.
 * </ul>
 */
public class TestDataPageV2EncryptionInterop {

  private static final String FIXTURES = "/encryption/datapagev2/";

  @TempDir
  private java.nio.file.Path tempDir;

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
    // The keys of the Arrow files are wrapped by the KMS client that generate_arrow_fixtures.py mirrors
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

  @ParameterizedTest(name = "{0}, directMemory={1}")
  @MethodSource("legacyFixtures")
  public void legacyFixturesHaveThePlaintextLevelsLayout(String fixture, boolean directMemory) throws Exception {
    // The test above only proves the legacy code path is covered if the fixtures really are legacy files
    List<DecodedPage> pages = decodeDataPagesV2(fixture(fixture), FOOTER_KEY, false);

    assertThat(pages).isNotEmpty();
    assertThat(pages).allSatisfy(page -> assertThat(page.singleModule).isFalse());
  }

  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("ciphersAndCodecs")
  public void writesTheLevelsInTheEncryptedPageBody(ParquetCipher cipher, CompressionCodecName codec)
      throws Exception {
    Path file = writeWithParquetJava(cipher, codec);

    List<DecodedPage> pages = decodeDataPagesV2(file, FOOTER_KEY, false);

    assertThat(pages).isNotEmpty();
    for (DecodedPage page : pages) {
      assertThat(page.singleModule)
          .as("the levels of %s must be inside the encrypted page body", page)
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
    Path file = writeWithParquetJava(ParquetCipher.AES_GCM_V1, CompressionCodecName.UNCOMPRESSED);
    DecodedPage page = decodeDataPagesV2(file, FOOTER_KEY, false).get(0);

    // Flip a bit of the first byte of the ciphertext, which is the first byte of the levels. When the
    // levels were stored in plaintext, changing them went unnoticed.
    File source = new File(file.toUri());
    try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(source, "rw")) {
      long position = page.bodyOffset + Integer.BYTES + AesCipher.NONCE_LENGTH;
      raf.seek(position);
      int original = raf.read();
      raf.seek(position);
      raf.write(original ^ 0x01);
    }
    // Drop the local filesystem CRC side-file so the tampered bytes reach the Parquet reader.
    Files.deleteIfExists(new File(source.getParent(), "." + source.getName() + ".crc").toPath());

    assertThatThrownBy(() -> readRows(file, new Configuration(), decryptionProperties(), false))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Can not read value at 0 in block -1")
        .hasRootCauseInstanceOf(TagVerificationException.class)
        .hasRootCauseMessage("GCM tag check failed");
  }

  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("ciphersAndCodecs")
  public void rewriterWritesTheLevelsInTheEncryptedPageBody(ParquetCipher cipher, CompressionCodecName codec)
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
    assertThat(pages).allSatisfy(page -> assertThat(page.singleModule).isTrue());
    assertThat(readRows(rewritten, new Configuration(), decryptionProperties(), false))
        .containsExactlyElementsOf(expectedRows());
    assertThat(readRows(rewritten, new Configuration(), decryptionProperties(), true))
        .containsExactlyElementsOf(expectedRows());
  }

  /** Writes a DataPageV2 file with 100 rows per page, encrypted with the footer key if there is a cipher. */
  private Path writeWithParquetJava(ParquetCipher cipher, CompressionCodecName codec) throws IOException {
    Path file = new Path(tempDir.resolve(UUID.randomUUID().toString()).toUri());
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(file)
        .withType(SCHEMA)
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withCompressionCodec(codec)
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

  private static Path fixture(String name) throws URISyntaxException {
    return new Path(TestDataPageV2EncryptionInterop.class
        .getResource(FIXTURES + name)
        .toURI());
  }

  /** What the reference decoder found out about one DataPageV2 page that has levels. */
  private static final class DecodedPage {
    private final String column;
    private final int repetitionLevelsLength;
    private final int definitionLevelsLength;
    private final long bodyOffset;
    private final boolean singleModule;
    private final int decryptedLength;
    private final int uncompressedPageSize;

    private DecodedPage(
        String column,
        int repetitionLevelsLength,
        int definitionLevelsLength,
        long bodyOffset,
        boolean singleModule,
        int decryptedLength,
        int uncompressedPageSize) {
      this.column = column;
      this.repetitionLevelsLength = repetitionLevelsLength;
      this.definitionLevelsLength = definitionLevelsLength;
      this.bodyOffset = bodyOffset;
      this.singleModule = singleModule;
      this.decryptedLength = decryptedLength;
      this.uncompressedPageSize = uncompressedPageSize;
    }

    private int levelsLength() {
      return repetitionLevelsLength + definitionLevelsLength;
    }

    @Override
    public String toString() {
      return "DataPageV2 of column " + column + " at offset " + bodyOffset + " with " + levelsLength()
          + " bytes of levels";
    }
  }

  /**
   * A decoder of the encrypted DataPageV2 pages of a file, written from the Parquet encryption
   * specification and sharing nothing with the reader of parquet-java except the cipher
   * implementations: it parses and decrypts the page headers itself, takes the data page module
   * from the page body, and decrypts it with the AAD of the specification.
   *
   * <p>As the specification has the page body start with the length of the encrypted module, the body
   * is a single module when the first four bytes hold the body length minus four. Otherwise the
   * levels are in plaintext and the module of the values follows them.
   *
   * @param file file with an encrypted footer, whose encrypted columns all use {@code columnKey}
   * @param onlyEncryptedColumns true to skip the columns that are not encrypted, for a file where
   *     only some of the columns are; false if all of the columns are encrypted
   * @return the pages that have repetition or definition levels, in file order
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
            long bodyOffset = bytes.length - in.available();
            byte[] body = new byte[header.getCompressed_page_size()];
            in.read(body);
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
                  BytesUtils.readIntLittleEndian(body, 0) == body.length - Integer.BYTES;
              byte[] module = singleModule ? body : Arrays.copyOfRange(body, levels, body.length);
              // Either way, what is decrypted starts with its own length
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
                    bodyOffset,
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
