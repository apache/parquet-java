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
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.io.api.Binary;

/**
 * Decides how a {@code FILE} value's payload is stored: inline in the value, or out of line as a
 * self-reference within the same Parquet file.
 *
 * <p>Object models hand over the resolved (logical) bytes and receive back a {@link Placement}
 * describing which fields of the {@code FILE} group to write. Callers do not choose between the two
 * forms themselves; the choice follows the configured threshold, so the same writing code produces
 * either form:
 *
 * <pre>{@code
 * FileValueWriter.Placement placement = fileValueWriter.write(payload);
 * if (placement.isInline()) {
 *   group.add("inline", placement.getInlineBytes());
 * } else {
 *   group.add("offset", placement.getOffset());
 *   group.add("size", placement.getSize());
 * }
 * }</pre>
 *
 * <p>Both forms describe the same logical bytes, so {@code content_type} and {@code checksum} are
 * written identically either way — they describe the resolved bytes, not the storage. Consumers see
 * no difference beyond which fields are set.
 *
 * <p>A self-reference payload is written immediately, while the record is being written and before
 * the row group's column chunks are flushed. It therefore lands in a contiguous run ahead of those
 * chunks, leaving each column chunk contiguous on disk. Writing eagerly is what makes the offset
 * knowable in time: {@code offset} and {@code size} are ordinary column values, and once a value has
 * been handed to a column writer it is encoded into a buffered page and cannot be revised, so a
 * placeholder could never be patched up later.
 *
 * @see SelfReferenceStorage
 */
public class FileValueWriter {

  /**
   * Where a {@code FILE} value's payload was placed, and therefore which fields of the {@code FILE}
   * group the caller should write. Either the payload is inline, or it is a self-reference located by
   * {@code offset} and {@code size}.
   */
  public static final class Placement {
    private final Binary inlineBytes;
    private final long offset;
    private final long size;

    private Placement(Binary inlineBytes, long offset, long size) {
      this.inlineBytes = inlineBytes;
      this.offset = offset;
      this.size = size;
    }

    static Placement inline(Binary inlineBytes) {
      return new Placement(inlineBytes, -1, -1);
    }

    static Placement selfReference(SelfReferenceStorage.StoredRange range) {
      return new Placement(null, range.getOffset(), range.getSize());
    }

    /** Whether the payload is stored inline, i.e. whether the {@code inline} field should be set. */
    public boolean isInline() {
      return inlineBytes != null;
    }

    /**
     * The bytes to write to the {@code inline} field.
     *
     * @throws IllegalStateException if the payload was stored as a self-reference
     */
    public Binary getInlineBytes() {
      if (!isInline()) {
        throw new IllegalStateException("Payload was stored as a self-reference, not inline");
      }
      return inlineBytes;
    }

    /**
     * The value to write to the {@code offset} field.
     *
     * @throws IllegalStateException if the payload was stored inline
     */
    public long getOffset() {
      if (isInline()) {
        throw new IllegalStateException("Payload was stored inline; it has no offset");
      }
      return offset;
    }

    /**
     * The value to write to the {@code size} field. This is the size of the stored representation
     * after compression and encryption, not the size of the resolved bytes.
     *
     * @throws IllegalStateException if the payload was stored inline
     */
    public long getSize() {
      if (isInline()) {
        throw new IllegalStateException("Payload was stored inline; it has no size");
      }
      return size;
    }
  }

  private final ParquetFileWriter fileWriter;
  private final CodecFactory.BytesCompressor inlineColumnCompressor;
  private final BlockCipher.Encryptor inlineColumnEncryptor;
  private final int inlineColumnOrdinal;
  private final int selfReferenceThreshold;

  /**
   * @param fileWriter the writer for the file being written; a block must be open when
   *     {@link #write} is called
   * @param inlineColumnCompressor the compressor for the {@code inline} column chunk's codec, whose
   *     compression a self-reference inherits
   * @param inlineColumnEncryptor the data-module encryptor of the {@code inline} column chunk, or
   *     {@code null} if that column chunk is not encrypted
   * @param inlineColumnOrdinal the ordinal of the {@code inline} column within the schema. The
   *     schema must declare {@code inline}: it is the reference point whose compression and
   *     encryption a self-reference inherits, so a schema without it can only store payloads inline
   *     or as external references. Note that the schema builder does not require {@code inline} for
   *     groups that declare {@code uri}, so an external-reference schema may reach here; pair such a
   *     schema with a threshold of {@link Integer#MAX_VALUE} so nothing is stored out of line.
   * @param selfReferenceThreshold payloads of at most this many bytes are stored inline; larger ones
   *     become self-references. See
   *     {@code ParquetProperties.Builder#withFileSelfReferenceThreshold(int)}.
   */
  public FileValueWriter(
      ParquetFileWriter fileWriter,
      CodecFactory.BytesCompressor inlineColumnCompressor,
      BlockCipher.Encryptor inlineColumnEncryptor,
      int inlineColumnOrdinal,
      int selfReferenceThreshold) {
    if (selfReferenceThreshold < 0) {
      throw new IllegalArgumentException(
          "Self-reference threshold must not be negative: " + selfReferenceThreshold);
    }
    if (inlineColumnOrdinal < 0) {
      throw new IllegalArgumentException("Invalid inline column ordinal: " + inlineColumnOrdinal);
    }
    this.fileWriter = fileWriter;
    this.inlineColumnCompressor = inlineColumnCompressor;
    this.inlineColumnEncryptor = inlineColumnEncryptor;
    this.inlineColumnOrdinal = inlineColumnOrdinal;
    this.selfReferenceThreshold = selfReferenceThreshold;
  }

  /**
   * Stores {@code payload} and returns which {@code FILE} group fields to write for it. Payloads at
   * or below the configured threshold are returned for inline storage; larger ones are written to the
   * file body immediately as self-references.
   *
   * <p>Must be called while a block is open on the underlying writer, and before that block's column
   * chunks are flushed.
   *
   * @param payload the resolved (logical) bytes of the value
   * @return the placement describing which fields to write
   * @throws IOException if writing the self-reference payload fails
   */
  public Placement write(Binary payload) throws IOException {
    if (payload == null) {
      throw new IllegalArgumentException("FILE payload must not be null");
    }
    if (payload.length() <= selfReferenceThreshold) {
      return Placement.inline(payload);
    }
    SelfReferenceStorage.StoredRange range = fileWriter.writeSelfReference(
        BytesInput.from(payload.toByteBuffer()),
        inlineColumnCompressor,
        inlineColumnEncryptor,
        inlineColumnOrdinal);
    return Placement.selfReference(range);
  }
}
