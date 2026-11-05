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
 * A still encrypted {@link DataPageV2} whose entire page body is, or if its layout is {@link
 * Layout#AMBIGUOUS} may be, a single encrypted module.
 *
 * <p>Parquet Modular Encryption encrypts the "data page" module, which for a DataPageV2 page is
 * the concatenation {@code [repetition levels | definition levels | (compressed) values]}. This is
 * what Arrow C++ and arrow-rs write, and what parquet-java writes since 1.19.0. Earlier
 * parquet-java versions left the levels in plaintext and encrypted only the values; those pages
 * are still read as plain {@link DataPageV2} pages whose levels are already separated out.
 *
 * <p>The page body can only be decrypted, and thereby split into levels and values, once the page
 * AAD is known. That happens in {@code ColumnChunkPageReader}. Until then the whole encrypted
 * module is held as the {@link #getData() data} of this page, and the repetition and definition
 * levels are empty: they must not be used before the page has been decrypted. The sizes of the
 * levels, which the page header carries, are kept separately.
 */
final class FullBodyEncryptedDataPageV2 extends DataPageV2 {

  /** How the body of an encrypted DataPageV2 page that has levels is laid out. */
  enum Layout {
    /** The whole body is one encrypted module, as in the specification. */
    SINGLE_MODULE,
    /** The levels are in plaintext, followed by an encrypted module of the values. */
    PLAINTEXT_LEVELS,
    /** The body fits both layouts, which can only be told apart by trying to decrypt it. */
    AMBIGUOUS
  }

  private final int repetitionLevelsByteLength;
  private final int definitionLevelsByteLength;
  private final DataPageV2 plaintextLevelsLayoutPage;

  /**
   * @param encryptedBody the encrypted page body, in the serialized form of an encrypted module:
   *                      {@code length (4 bytes) | nonce | ciphertext [| tag]}
   * @param uncompressedSize the uncompressed page size of the page header, levels included
   * @param plaintextLevelsLayoutPage the same page with the levels separated out, if the page
   *                                  body is {@link Layout#AMBIGUOUS}, otherwise null
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

  /**
   * @return the page as it would be if its body had the {@link Layout#PLAINTEXT_LEVELS} layout, to
   *         fall back to when decrypting it as a single module fails. Null unless the layout of the
   *         page body is {@link Layout#AMBIGUOUS}.
   */
  DataPageV2 getPlaintextLevelsLayoutPage() {
    return plaintextLevelsLayoutPage;
  }

  /**
   * Tells which of the two layouts the body of an encrypted DataPageV2 page has. It must only be
   * called for a page that has repetition or definition levels: without levels both layouts are
   * byte-for-byte identical.
   *
   * <p>An encrypted module starts with its own length, a 4 byte little endian integer holding the
   * size of the rest of the module. Each layout therefore announces itself:
   *
   * <ul>
   *   <li>if the page body is a single module, the first four bytes hold the size of the body minus
   *       four;
   *   <li>if the levels are stored in plaintext, the module of the values starts right after the
   *       levels, and the four bytes there hold the size of the body, minus the size of the levels,
   *       minus four.
   * </ul>
   *
   * <p>A page body that is a single module has a random nonce and ciphertext after its length, so
   * it matches the second condition only by a coincidence of about one in 2^32, a little more often if
   * the levels take fewer than four bytes, as the four bytes then overlap the length of the module. A page body that has
   * plaintext levels starts with RLE encoded levels, so it matches the first condition only if
   * those happen to spell out the size of the page. When both conditions are met, the layout can
   * only be found by decrypting the page, see {@link Layout#AMBIGUOUS}. When neither is met the page
   * is corrupt, and it is treated as a single module so that decryption reports the problem.
   *
   * @param pageBody the page body, as sized by the {@code compressed_page_size} of the page header
   * @param levelsByteLength the size of the repetition and definition levels, which is above zero
   * @return the layout of the page body
   * @throws IOException if the page body cannot be read
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
