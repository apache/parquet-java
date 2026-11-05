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
 * A still encrypted {@link DataPageV2} whose entire page body is a single encrypted module.
 *
 * <p>Parquet Modular Encryption encrypts the "data page" module, which for a DataPageV2 page is
 * the concatenation {@code [repetition levels | definition levels | (compressed) values]}. This is
 * what Arrow C++ and arrow-rs write, and what parquet-java writes since the layout was aligned.
 * Older parquet-java versions left the levels in plaintext and encrypted only the values; those
 * pages are still read as plain {@link DataPageV2} pages whose levels are already separated out.
 *
 * <p>The page body can only be decrypted, and thereby split into levels and values, once the page
 * AAD is known. That happens in {@code ColumnChunkPageReader}. Until then the whole encrypted
 * module is held as the {@link #getData() data} of this page, and the repetition and definition
 * levels are empty. The sizes of the levels, which the page header carries, are kept separately.
 */
final class FullBodyEncryptedDataPageV2 extends DataPageV2 {

  private final int repetitionLevelsByteLength;
  private final int definitionLevelsByteLength;

  /**
   * @param encryptedBody the encrypted page body, in the serialized form of an encrypted module:
   *                      {@code length (4 bytes) | nonce | ciphertext [| tag]}
   * @param uncompressedSize the uncompressed page size of the page header, levels included
   */
  FullBodyEncryptedDataPageV2(
      int rowCount,
      int nullCount,
      int valueCount,
      BytesInput encryptedBody,
      Encoding dataEncoding,
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
        encryptedBody,
        uncompressedSize,
        statistics,
        isCompressed);
    this.repetitionLevelsByteLength = repetitionLevelsByteLength;
    this.definitionLevelsByteLength = definitionLevelsByteLength;
  }

  int getRepetitionLevelsByteLength() {
    return repetitionLevelsByteLength;
  }

  int getDefinitionLevelsByteLength() {
    return definitionLevelsByteLength;
  }

  /**
   * Tells which of the two DataPageV2 layouts an encrypted page body has. This must only be called
   * for a page that has repetition or definition levels: without levels both layouts are
   * byte-for-byte identical.
   *
   * <p>An encrypted module starts with its own length, a 4 byte little endian integer holding the
   * size of the rest of the module. If the page body is a single module, that length is the size
   * of the page body minus 4. If the levels are stored in plaintext, which is how earlier
   * parquet-java versions wrote the page, the body starts with the RLE encoded levels instead, and
   * the length of the encrypted values is found after the levels. Matching the first four bytes
   * with the page size is therefore sufficient to recognize the single module layout. A legacy page
   * would only be mistaken for it if the first four bytes of its levels happened to hold exactly
   * that value; in that case the page fails to decrypt, it is not silently misread.
   *
   * @param pageBody the page body, as sized by the {@code compressed_page_size} of the page header
   * @return true if the whole page body is one encrypted module, false if the levels are stored in
   *         plaintext in front of the encrypted values
   * @throws IOException if the page body cannot be read
   */
  static boolean isSingleEncryptedModule(BytesInput pageBody) throws IOException {
    long size = pageBody.size();
    if (size < Integer.BYTES) {
      return false;
    }
    try (ByteBufferInputStream in = pageBody.toInputStream()) {
      return BytesUtils.readIntLittleEndian(in) == size - Integer.BYTES;
    }
  }
}
