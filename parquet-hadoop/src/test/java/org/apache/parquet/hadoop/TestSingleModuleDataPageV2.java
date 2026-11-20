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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.AesMode;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.hadoop.ColumnChunkPageReadStore.ColumnChunkPageReader;
import org.apache.parquet.hadoop.SingleModuleDataPageV2.Layout;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestSingleModuleDataPageV2 {

  private static final String PARQUET_JAVA_1_18 = "parquet-mr version 1.18.0 (build abc)";
  private static final String ARROW = "parquet-cpp-arrow version 24.0.0";

  /** A bit-packed run of 8 levels, then an RLE run of 8 levels. */
  private static final byte[] LEVELS = {0x03, (byte) 0xff, 0x10, 0x01};

  /** {@code contentLength} as a 4 byte LE int, followed by that many bytes. */
  private static byte[] module(int contentLength) {
    ByteBuffer module = ByteBuffer.allocate(Integer.BYTES + contentLength).order(ByteOrder.LITTLE_ENDIAN);
    module.putInt(contentLength);
    for (int i = 0; i < contentLength; i++) {
      module.put((byte) (0x80 + i));
    }
    return module.array();
  }

  private static byte[] concat(byte[] first, byte[] second) {
    byte[] result = new byte[first.length + second.length];
    System.arraycopy(first, 0, result, 0, first.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  @Test
  public void pageThatIsOneModuleIsASingleModule() throws IOException {
    assertThat(SingleModuleDataPageV2.detectLayout(BytesInput.from(module(50)), LEVELS.length, PARQUET_JAVA_1_18))
        .isEqualTo(Layout.SINGLE_MODULE);
  }

  @Test
  public void pageWithPlaintextLevelsInFrontOfAModuleHasPlaintextLevels() throws IOException {
    byte[] pageBytes = concat(LEVELS, module(50));

    assertThat(SingleModuleDataPageV2.detectLayout(BytesInput.from(pageBytes), LEVELS.length, ARROW))
        .isEqualTo(Layout.PLAINTEXT_LEVELS);
  }

  @Test
  public void pageThatIsOneModuleIsRecognizedWhenItIsNotInOneBuffer() throws IOException {
    byte[] pageBytes = module(300);
    ByteBuffer first = ByteBuffer.wrap(pageBytes, 0, 2).slice();
    ByteBuffer second = ByteBuffer.wrap(pageBytes, 2, pageBytes.length - 2).slice();

    assertThat(SingleModuleDataPageV2.detectLayout(
            BytesInput.from(first, second), LEVELS.length, PARQUET_JAVA_1_18))
        .isEqualTo(Layout.SINGLE_MODULE);
  }

  @Test
  public void pageWithPlaintextLevelsIsRecognizedWhenTheModuleLengthIsNotInOneBuffer() throws IOException {
    byte[] pageBytes = concat(LEVELS, module(300));
    // Split inside the module length
    ByteBuffer first = ByteBuffer.wrap(pageBytes, 0, LEVELS.length + 2).slice();
    ByteBuffer second = ByteBuffer.wrap(pageBytes, LEVELS.length + 2, pageBytes.length - LEVELS.length - 2)
        .slice();

    assertThat(SingleModuleDataPageV2.detectLayout(BytesInput.from(first, second), LEVELS.length, ARROW))
        .isEqualTo(Layout.PLAINTEXT_LEVELS);
  }

  /** Levels whose first 4 bytes are {@code pageSize - 4}, followed by a module. */
  private static byte[] pageThatFitsBothLayouts() {
    byte[] module = module(50);
    int pageSize = LEVELS.length + module.length;
    byte[] levels = ByteBuffer.allocate(LEVELS.length)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(pageSize - Integer.BYTES)
        .array();
    return concat(levels, module);
  }

  @Test
  public void pageThatFitsBothLayoutsHasPlaintextLevelsIfWrittenByParquetJava118() throws IOException {
    assertThat(SingleModuleDataPageV2.detectLayout(
            BytesInput.from(pageThatFitsBothLayouts()), LEVELS.length, PARQUET_JAVA_1_18))
        .isEqualTo(Layout.PLAINTEXT_LEVELS);
  }

  @Test
  public void pageThatFitsBothLayoutsIsASingleModuleIfWrittenByAnotherWriter() throws IOException {
    assertThat(SingleModuleDataPageV2.detectLayout(
            BytesInput.from(pageThatFitsBothLayouts()), LEVELS.length, ARROW))
        .isEqualTo(Layout.SINGLE_MODULE);
  }

  @Test
  public void pageThatFitsNeitherLayoutIsTreatedAsASingleModuleSoThatDecryptionReportsIt() throws IOException {
    byte[] pageBytes = concat(LEVELS, module(50));
    pageBytes[LEVELS.length] ^= 0x01;

    assertThat(SingleModuleDataPageV2.detectLayout(BytesInput.from(pageBytes), LEVELS.length, PARQUET_JAVA_1_18))
        .isEqualTo(Layout.SINGLE_MODULE);
  }

  @Test
  public void pageTooShortForAModuleLengthIsASingleModule() throws IOException {
    assertThat(SingleModuleDataPageV2.detectLayout(BytesInput.empty(), 1, PARQUET_JAVA_1_18))
        .isEqualTo(Layout.SINGLE_MODULE);
    assertThat(SingleModuleDataPageV2.detectLayout(BytesInput.from(new byte[] {1, 2, 3}), 1, PARQUET_JAVA_1_18))
        .isEqualTo(Layout.SINGLE_MODULE);
    // Page is only levels
    assertThat(SingleModuleDataPageV2.detectLayout(BytesInput.from(LEVELS), LEVELS.length, PARQUET_JAVA_1_18))
        .isEqualTo(Layout.SINGLE_MODULE);
  }

  @ParameterizedTest
  @CsvSource({
    "'parquet-mr version 1.18.0 (build abc)', PLAINTEXT_LEVELS",
    "'parquet-mr version 1.19.0-SNAPSHOT (build abc)', PLAINTEXT_LEVELS",
    "'parquet-mr version 1.19.0 (build abc)', SINGLE_MODULE",
    "'parquet-cpp-arrow version 24.0.0', SINGLE_MODULE",
    "'parquet-mr version not-a-version', SINGLE_MODULE",
    "'not a created_by', SINGLE_MODULE",
    "'', SINGLE_MODULE",
  })
  public void layoutWrittenByFollowsTheWriterVersion(String createdBy, Layout expected) {
    assertThat(SingleModuleDataPageV2.layoutWrittenBy(createdBy)).isEqualTo(expected);
  }

  @Test
  public void layoutWrittenByAnUnknownWriterIsASingleModule() {
    assertThat(SingleModuleDataPageV2.layoutWrittenBy(null)).isEqualTo(Layout.SINGLE_MODULE);
  }

  private static DataPage readUncompressedPage(int uncompressedPageSize) {
    return readUncompressedPage(LEVELS.length, uncompressedPageSize);
  }

  /**
   * Reads one uncompressed page whose decrypted module is {@code LEVELS} and 8 value bytes, and whose
   * header gives {@code definitionLevelsByteLength} bytes of definition levels.
   */
  private static DataPage readUncompressedPage(int definitionLevelsByteLength, int uncompressedPageSize) {
    byte[] key = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    byte[] fileAAD = "file".getBytes(StandardCharsets.UTF_8);
    byte[] plainPage = concat(LEVELS, new byte[8]);
    byte[] dataPageAAD = AesCipher.createModuleAAD(fileAAD, ModuleType.DataPage, 0, 0, 0);
    byte[] encryptedModule =
        ModuleCipherFactory.getEncryptor(AesMode.GCM, key).encrypt(plainPage, dataPageAAD);
    SingleModuleDataPageV2 page = new SingleModuleDataPageV2(
        8,
        0,
        8,
        Encoding.PLAIN,
        BytesInput.from(encryptedModule),
        0,
        definitionLevelsByteLength,
        uncompressedPageSize,
        null,
        false);
    ColumnChunkPageReader reader = new ColumnChunkPageReader(
        null,
        Collections.singletonList(page),
        null,
        null,
        8,
        ModuleCipherFactory.getDecryptor(AesMode.GCM, key),
        fileAAD,
        0,
        0,
        ParquetReadOptions.builder().build());
    return reader.readPage();
  }

  @Test
  public void uncompressedPageWithTheSizeOfItsDecryptedModuleIsRead() {
    DataPageV2 page = (DataPageV2) readUncompressedPage(LEVELS.length + 8);

    assertThat(page.getDefinitionLevels().size()).isEqualTo(LEVELS.length);
    assertThat(page.getData().size()).isEqualTo(8);
  }

  @Test
  public void uncompressedPageWhoseHeaderDisagreesWithItsDecryptedModuleIsRejected() {
    assertThatThrownBy(() -> readUncompressedPage(LEVELS.length + 9))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage(
            "Decrypted DataPageV2 page has 12 bytes, but the page header gives an uncompressed page size"
                + " of 13 bytes");
  }

  @Test
  public void pageWhoseDecryptedModuleIsShorterThanItsLevelsIsRejected() {
    assertThatThrownBy(() -> readUncompressedPage(LEVELS.length + 9, LEVELS.length + 9))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Decrypted DataPageV2 page has 12 bytes, which is less than the 13 bytes of repetition"
            + " and definition levels in the page header");
  }
}
