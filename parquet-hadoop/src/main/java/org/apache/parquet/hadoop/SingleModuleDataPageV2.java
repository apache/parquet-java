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
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.Strings;
import org.apache.parquet.VersionParser;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.VersionParser.VersionParseException;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.statistics.Statistics;

/**
 * An encrypted DataPageV2 whose levels and values are one encrypted module. Until {@code
 * ColumnChunkPageReader} decrypts the module and splits it using the level lengths, {@link
 * #getData()} holds the whole module and the levels are empty.
 */
final class SingleModuleDataPageV2 extends DataPageV2 {

  /** How the bytes after the header of an encrypted DataPageV2 page that has levels are laid out. */
  enum Layout {
    /** {@code [module of levels and values]} */
    SINGLE_MODULE,
    /** {@code [plaintext levels][module of values]} */
    PLAINTEXT_LEVELS
  }

  private static final SemanticVersion SINGLE_MODULE_SINCE = new SemanticVersion(1, 19, 0);

  private final int repetitionLevelsByteLength;
  private final int definitionLevelsByteLength;

  /**
   * @param encryptedModule {@code length (4 bytes, LE) | nonce | ciphertext [| tag]}
   * @param uncompressedSize uncompressed page size, levels included
   */
  SingleModuleDataPageV2(
      int rowCount,
      int nullCount,
      int valueCount,
      Encoding dataEncoding,
      BytesInput encryptedModule,
      int repetitionLevelsByteLength,
      int definitionLevelsByteLength,
      int uncompressedSize,
      Statistics<?> statistics,
      boolean isCompressed) {
    super(
        rowCount,
        nullCount,
        valueCount,
        BytesInput.empty(),
        BytesInput.empty(),
        dataEncoding,
        encryptedModule,
        uncompressedSize,
        statistics,
        isCompressed);
    this.repetitionLevelsByteLength = repetitionLevelsByteLength;
    this.definitionLevelsByteLength = definitionLevelsByteLength;
  }

  BytesInput getEncryptedModule() {
    return getData();
  }

  int getRepetitionLevelsByteLength() {
    return repetitionLevelsByteLength;
  }

  int getDefinitionLevelsByteLength() {
    return definitionLevelsByteLength;
  }

  /**
   * Detects the layout of the bytes after the header of a page that has levels. A module starts with the length of the
   * rest of the module as a 4 byte LE int, so:
   *
   * <ul>
   *   <li>{@link Layout#SINGLE_MODULE}: the int at offset 0 is {@code size - 4}
   *   <li>{@link Layout#PLAINTEXT_LEVELS}: the int at offset {@code levelsByteLength} is {@code size -
   *       levelsByteLength - 4}
   * </ul>
   *
   * If both hold, the layout is the one written by {@code createdBy}. If neither holds, the page is
   * corrupt and {@link Layout#SINGLE_MODULE} is returned, so that decryption fails.
   *
   * @param pageBytes the bytes after the page header, {@code compressed_page_size} of them
   * @param levelsByteLength the length of the repetition and definition levels, above zero
   * @param createdBy the {@code created_by} of the file
   */
  static Layout detectLayout(BytesInput pageBytes, int levelsByteLength, String createdBy) throws IOException {
    long size = pageBytes.size();
    boolean singleModule = size >= Integer.BYTES && readLength(pageBytes, 0) == size - Integer.BYTES;
    boolean plaintextLevels = size - levelsByteLength >= Integer.BYTES
        && readLength(pageBytes, levelsByteLength) == size - levelsByteLength - Integer.BYTES;
    if (plaintextLevels && singleModule) {
      return layoutWrittenBy(createdBy);
    }
    return plaintextLevels ? Layout.PLAINTEXT_LEVELS : Layout.SINGLE_MODULE;
  }

  /**
   * Returns the layout written by the application in {@code created_by}: {@link
   * Layout#PLAINTEXT_LEVELS} for parquet-java before 1.19.0, pre-releases of 1.19.0 included, and
   * {@link Layout#SINGLE_MODULE} otherwise, including if {@code created_by} is missing or cannot be
   * parsed.
   */
  static Layout layoutWrittenBy(String createdBy) {
    if (Strings.isNullOrEmpty(createdBy)) {
      return Layout.SINGLE_MODULE;
    }
    ParsedVersion writer;
    try {
      writer = VersionParser.parse(createdBy);
    } catch (VersionParseException | RuntimeException e) {
      return Layout.SINGLE_MODULE;
    }
    if ("parquet-mr".equals(writer.application)
        && writer.hasSemanticVersion()
        && writer.getSemanticVersion().compareTo(SINGLE_MODULE_SINCE) < 0) {
      return Layout.PLAINTEXT_LEVELS;
    }
    return Layout.SINGLE_MODULE;
  }

  private static int readLength(BytesInput pageBytes, int offset) throws IOException {
    try (ByteBufferInputStream in = pageBytes.toInputStream()) {
      in.skipFully(offset);
      return BytesUtils.readIntLittleEndian(in);
    }
  }
}
