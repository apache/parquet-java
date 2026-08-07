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
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Implements the storage-inheritance semantics for {@code FILE} self-references as specified in the
 * Parquet format (see {@code LogicalTypes.md}, section "FILE"). A self-reference is a {@code FILE}
 * value whose {@code uri} is not set and that locates a byte range within the same Parquet file via
 * {@code offset} and {@code size}.
 *
 * <p>A self-reference does not point at the resolved (logical) bytes directly. Instead it points at
 * a <i>stored representation</i>: the resolved bytes after being compressed and (optionally)
 * encrypted, inheriting the {@link CompressionCodecName} and encryption settings of the
 * {@code inline} column chunk in the same row group. Each self-reference is an independent
 * compression block and (when encrypted) an independent encryption module; state is not shared with
 * data pages or with other self-references.
 *
 * <p>Layout of a stored self-reference:
 *
 * <ul>
 *   <li><b>Unencrypted:</b> the compressed block (or the raw bytes when the codec is
 *       {@link CompressionCodecName#UNCOMPRESSED}). {@code offset}/{@code size} cover exactly these
 *       bytes.
 *   <li><b>Encrypted:</b> the modular-encryption serialization of the compressed block — a 4-byte
 *       little-endian length, a 12-byte nonce, the ciphertext, and (for AES_GCM_V1) a 16-byte GCM
 *       tag. {@code offset} points to the beginning of the 4-byte length and {@code size} covers the
 *       complete encrypted module. The AAD uses the {@code Self-Reference} module type (10) with the
 *       8-byte file offset of the stored representation; see
 *       {@link AesCipher#createSelfReferenceAAD}.
 * </ul>
 *
 * <p>Because the AAD is keyed on the file offset — a value the {@code FILE} group already carries in
 * its {@code offset} field — a reader can resolve a self-reference directly from the value, without
 * decoding the pages preceding it. An encrypted stored representation is therefore bound to one
 * column chunk at one offset and must not be shared between column chunks.
 *
 * <p>Compression is always applied before encryption on write; decryption is applied before
 * decompression on read.
 */
public final class SelfReferenceStorage {

  /**
   * The largest encrypted module a writer can serialize: the 4-byte little-endian length field is
   * read back as a signed int, so the buffer it describes cannot exceed 2 GiB.
   */
  public static final long MAX_ENCRYPTED_MODULE_SIZE = Integer.MAX_VALUE;

  /**
   * Bytes an encrypted module adds around the compressed block: the 4-byte length, the 12-byte
   * nonce, and the 16-byte GCM tag. AES_GCM_CTR_V1 omits the tag, so this is an upper bound.
   */
  private static final long MAX_ENCRYPTION_OVERHEAD = 4 + 12 + 16;

  private SelfReferenceStorage() {}

  /**
   * The location of a stored self-reference within the Parquet file. The {@code offset} and
   * {@code size} are exactly the values a writer records in the {@code offset} and {@code size}
   * fields of the {@code FILE} group.
   */
  public static final class StoredRange {
    private final long offset;
    private final long size;

    public StoredRange(long offset, long size) {
      this.offset = offset;
      this.size = size;
    }

    /** The byte offset of the stored representation within the Parquet file. */
    public long getOffset() {
      return offset;
    }

    /** The byte length of the stored representation. */
    public long getSize() {
      return size;
    }
  }

  /**
   * Compresses (and optionally encrypts) {@code resolvedBytes} as an independent stored block and
   * appends it to {@code out}, returning the {@link StoredRange} that a writer records in the
   * {@code offset} and {@code size} fields of the self-reference.
   *
   * @param resolvedBytes the resolved (logical) bytes of the self-reference
   * @param compressor the compressor for the {@code inline} column chunk's codec; must not be null
   *     (use the {@link CompressionCodecName#UNCOMPRESSED} compressor to store bytes uncompressed)
   * @param pageBlockEncryptor the data-module encryptor of the {@code inline} column chunk, or
   *     {@code null} if the column chunk is not encrypted
   * @param fileAAD the file AAD, required when {@code pageBlockEncryptor} is non-null
   * @param rowGroupOrdinal the row group ordinal of the self-reference
   * @param columnOrdinal the ordinal of the {@code inline} column the self-reference inherits from
   * @param out the Parquet file output stream, positioned where the stored block should be written
   * @return the offset and size of the stored representation
   * @throws IOException if writing or compression fails
   */
  public static StoredRange write(
      BytesInput resolvedBytes,
      BytesCompressor compressor,
      BlockCipher.Encryptor pageBlockEncryptor,
      byte[] fileAAD,
      int rowGroupOrdinal,
      int columnOrdinal,
      org.apache.parquet.io.PositionOutputStream out)
      throws IOException {

    // Step 1: compress the resolved bytes as an independent compression block. UNCOMPRESSED leaves
    // the bytes unchanged (the NO_OP_COMPRESSOR returns its input).
    BytesInput stored = compressor.compress(resolvedBytes);

    // The offset of the stored representation is the current stream position, and it is also the
    // AAD's self-reference identity, so it must be read before anything is written.
    long offset = out.getPos();

    // Step 2: when the inline column chunk is encrypted, encrypt the compressed block as an
    // independent module keyed on that offset. The encryptor prepends the 4-byte length and the
    // nonce and appends the GCM tag (for AES_GCM_V1); the returned byte array is the complete
    // stored module.
    if (pageBlockEncryptor != null) {
      long plaintextSize = stored.size();
      // The 4-byte length field of an encrypted module caps the buffer at 2 GiB. Check before
      // encrypting so an oversized value fails with a diagnostic instead of a corrupt length.
      long encryptedSize = plaintextSize + MAX_ENCRYPTION_OVERHEAD;
      if (encryptedSize > MAX_ENCRYPTED_MODULE_SIZE) {
        throw new IllegalArgumentException("Self-reference is too large to encrypt: " + plaintextSize
            + " compressed bytes exceed the " + MAX_ENCRYPTED_MODULE_SIZE
            + "-byte limit imposed by the 4-byte length field of an encrypted module. "
            + "Store this value as an external reference (uri) instead.");
      }
      byte[] selfReferenceAAD = AesCipher.createSelfReferenceAAD(fileAAD, rowGroupOrdinal, columnOrdinal, offset);
      stored = BytesInput.from(pageBlockEncryptor.encrypt(stored.toByteArray(), selfReferenceAAD));
    }

    long size = stored.size();
    stored.writeAllTo(out);
    return new StoredRange(offset, size);
  }

  /**
   * Resolves a stored self-reference back to its logical bytes: decrypts the stored representation
   * (when the {@code inline} column chunk is encrypted) and then decompresses it using the column
   * chunk's codec.
   *
   * @param storedBytes the stored representation, i.e. the {@code [offset, offset + size)} range
   *     read from the Parquet file
   * @param codecName the {@link CompressionCodecName} of the {@code inline} column chunk
   * @param codecFactory the codec factory used to decompress the block
   * @param pageBlockDecryptor the data-module decryptor of the {@code inline} column chunk, or
   *     {@code null} if the column chunk is not encrypted
   * @param fileAAD the file AAD, required when {@code pageBlockDecryptor} is non-null
   * @param rowGroupOrdinal the row group ordinal of the self-reference
   * @param columnOrdinal the ordinal of the {@code inline} column the self-reference inherits from
   * @param selfReferenceOffset the value of the self-reference's {@code offset} field, which is both
   *     where {@code storedBytes} was read from and the self-reference's AAD identity
   * @return the resolved (logical) bytes
   * @throws IOException if decompression fails
   */
  public static BytesInput resolve(
      BytesInput storedBytes,
      CompressionCodecName codecName,
      CodecFactory codecFactory,
      BlockCipher.Decryptor pageBlockDecryptor,
      byte[] fileAAD,
      int rowGroupOrdinal,
      int columnOrdinal,
      long selfReferenceOffset)
      throws IOException {

    BytesInput compressed = storedBytes;

    // Step 1: decrypt when the inline column chunk is encrypted. The decryptor consumes the 4-byte
    // length, nonce, ciphertext, and GCM tag and returns the compressed block. The AAD is rebuilt
    // from the offset alone, so no state from preceding values is needed.
    if (pageBlockDecryptor != null) {
      byte[] selfReferenceAAD =
          AesCipher.createSelfReferenceAAD(fileAAD, rowGroupOrdinal, columnOrdinal, selfReferenceOffset);
      compressed = BytesInput.from(pageBlockDecryptor.decrypt(storedBytes.toByteArray(), selfReferenceAAD));
    }

    // Step 2: decompress. The resolved size is not stored, so the codec decompresses into a
    // dynamically sized buffer.
    return codecFactory.decompressUnknownSize(codecName, compressed);
  }
}
