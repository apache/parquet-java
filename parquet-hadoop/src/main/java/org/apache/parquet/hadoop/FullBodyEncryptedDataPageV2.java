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

import java.io.IOException;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.statistics.Statistics;

/**
 * An encrypted DataPageV2 whose body, levels included, is one encrypted module. The body is held as
 * {@link #getData()} and the levels are empty until {@code ColumnChunkPageReader} decrypts the page
 * and splits the body using the level lengths.
 */
final class FullBodyEncryptedDataPageV2 extends DataPageV2 {

  /** How the body of an encrypted DataPageV2 page that has levels is laid out. */
  enum Layout {
    /** {@code [module of levels and values]} */
    SINGLE_MODULE,
    /** {@code [plaintext levels][module of values]} */
    PLAINTEXT_LEVELS,
    /** Both length checks match; only decryption can tell. */
    AMBIGUOUS
  }

  private final int repetitionLevelsByteLength;
  private final int definitionLevelsByteLength;
  private final DataPageV2 plaintextLevelsLayoutPage;

  /**
   * @param encryptedBody {@code length (4 bytes, LE) | nonce | ciphertext [| tag]}
   * @param uncompressedSize uncompressed page size, levels included
   * @param plaintextLevelsLayoutPage the page split as {@link Layout#PLAINTEXT_LEVELS}, if the layout is
   *     {@link Layout#AMBIGUOUS}; otherwise null
   */
  FullBodyEncryptedDataPageV2(
      int rowCount,
      int nullCount,
      int valueCount,
      Encoding dataEncoding,
      BytesInput encryptedBody,
      int repetitionLevelsByteLength,
      int definitionLevelsByteLength,
      int uncompressedSize,
      Statistics<?> statistics,
      boolean isCompressed,
      DataPageV2 plaintextLevelsLayoutPage) {
    super(
        rowCount,
        nullCount,
        valueCount,
        BytesInput.empty(),
        BytesInput.empty(),
        dataEncoding,
        encryptedBody,
        uncompressedSize,
        statistics,
        isCompressed);
    this.repetitionLevelsByteLength = repetitionLevelsByteLength;
    this.definitionLevelsByteLength = definitionLevelsByteLength;
    this.plaintextLevelsLayoutPage = plaintextLevelsLayoutPage;
  }

  int getRepetitionLevelsByteLength() {
    return repetitionLevelsByteLength;
  }

  int getDefinitionLevelsByteLength() {
    return definitionLevelsByteLength;
  }

  /** @return the page split as {@link Layout#PLAINTEXT_LEVELS}, or null if the layout is not ambiguous */
  DataPageV2 getPlaintextLevelsLayoutPage() {
    return plaintextLevelsLayoutPage;
  }

  @Override
  public void setCrc(int crc) {
    super.setCrc(crc);
    if (plaintextLevelsLayoutPage != null) {
      plaintextLevelsLayoutPage.setCrc(crc);
    }
  }

  /**
   * Detects the layout of the body of a page that has levels. A module starts with the length of the
   * rest of the module as a 4 byte LE int, so:
   *
   * <ul>
   *   <li>{@link Layout#SINGLE_MODULE}: the int at offset 0 is {@code size - 4}
   *   <li>{@link Layout#PLAINTEXT_LEVELS}: the int at offset {@code levelsByteLength} is {@code size -
   *       levelsByteLength - 4}
   *   <li>{@link Layout#AMBIGUOUS}: both hold
   * </ul>
   *
   * If neither holds, the page is corrupt and {@link Layout#SINGLE_MODULE} is returned, so that
   * decryption fails.
   *
   * @param pageBody the page body, of {@code compressed_page_size} bytes
   * @param levelsByteLength the length of the repetition and definition levels, above zero
   */
  static Layout detectLayout(BytesInput pageBody, int levelsByteLength) throws IOException {
    long size = pageBody.size();
    boolean singleModule = size >= Integer.BYTES && readLength(pageBody, 0) == size - Integer.BYTES;
    boolean plaintextLevels = size - levelsByteLength >= Integer.BYTES
        && readLength(pageBody, levelsByteLength) == size - levelsByteLength - Integer.BYTES;
    if (plaintextLevels && singleModule) {
      return Layout.AMBIGUOUS;
    }
    return plaintextLevels ? Layout.PLAINTEXT_LEVELS : Layout.SINGLE_MODULE;
  }

  private static int readLength(BytesInput pageBody, int offset) throws IOException {
    try (ByteBufferInputStream in = pageBody.toInputStream()) {
      in.skipFully(offset);
      return BytesUtils.readIntLittleEndian(in);
    }
  }
}
