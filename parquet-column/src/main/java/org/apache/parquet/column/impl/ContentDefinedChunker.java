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
package org.apache.parquet.column.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ContentDefinedChunkingOptions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Content defined chunking (CDC) divides data into variable-sized chunks based on the
 * content of the data itself, rather than using fixed-size boundaries.
 *
 * <p>For example, given this sequence of values in a column:
 *
 * <pre>
 * File1:    [1,2,3,   4,5,6,   7,8,9]
 *            chunk1   chunk2   chunk3
 * </pre>
 *
 * <p>Assume there is an inserted value between 3 and 4:
 *
 * <pre>
 * File2:     [1,2,3,0,  4,5,6,   7,8,9]
 *            new-chunk  chunk2   chunk3
 * </pre>
 *
 * <p>The chunking process will adjust to maintain stable boundaries across data modifications. Each
 * chunk defines a new parquet data page which is contiguously written out to the file. Since each page
 * is compressed independently, the files' contents would look like the following with unique page
 * identifiers:
 *
 * <pre>
 * File1:     [Page1][Page2][Page3]...
 * File2:     [Page4][Page2][Page3]...
 * </pre>
 *
 * <p>Then the parquet file is uploaded to a content addressable storage (CAS) system which splits the
 * byte stream into content defined blobs. The CAS system calculates a unique identifier for each blob,
 * then stores the blob in a key-value store. If the same blob is encountered again, the system can
 * refer to the hash instead of physically storing the blob again. In the example above, the CAS system
 * would store Page1, Page2, Page3 and Page4 only once and the required metadata to reassemble the
 * files. While the deduplication is performed by the CAS system, the parquet chunker makes it possible
 * to efficiently deduplicate the data by consistently dividing the data into chunks.
 *
 * <p>Implementation details: only the parquet writer must be aware of the content defined chunking,
 * the reader doesn't need to know about it. The file writer holds one chunker per leaf column, made by
 * {@link #forColumns}, and passes it to the column writers of every row group. The chunker's state is
 * maintained across the entire column without being reset between pages and row groups.
 *
 * <p>The column writer passes the (repetition level, definition level, value) triplets to the chunker
 * one by one, which adjusts the column-global rolling hash based on each triplet. Whenever the rolling
 * hash matches a predefined mask, a new chunk starts, and the column writer ends its page before that
 * triplet. Note that the boundaries are deterministically calculated exclusively based on the data
 * itself, so the same data will always produce the same chunks, given the same options.
 *
 * <p>Ported from Arrow C++ {@code parquet/chunker_internal} and arrow-rs {@code column/chunker/cdc.rs},
 * with the same boundaries for the same values, except where Arrow holds them in another form, such as
 * a DECIMAL, an INT96 or a dictionary array. Public for the file writers of other modules only. Not
 * thread safe.
 *
 * <p>References:
 *
 * <ul>
 *   <li>FastCDC: a Fast and Efficient Content-Defined Chunking Approach for Data Deduplication,
 *       https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf
 *   <li>Git is for Data (chunk size normalization used here is described in section 6.2.1),
 *       https://www.cidrdb.org/cidr2023/papers/p43-low.pdf
 * </ul>
 */
public final class ContentDefinedChunker {

  // Minimum chunk size in bytes, the rolling hash will not be updated until this size is reached for
  // each chunk. Note that all data sent through the hash function is counted towards the chunk size,
  // including definition and repetition levels.
  private final long minChunkSize;
  private final long maxChunkSize;
  // The mask to match the rolling hash against to determine if a new chunk should be created. The mask
  // is calculated based on min/max chunk size and the normalization level.
  private final long rollingHashMask;
  private final int maxDefinitionLevel;
  private final int maxRepetitionLevel;
  private final int byteWidth;

  // Rolling hash state, never reset, only initialized once for the entire column.
  private long rollingHash;
  // Whether the rolling hash has matched the mask since the last chunk creation. This flag is set to
  // true by roll() when the mask is matched and reset to false by needNewChunk().
  private boolean hasMatched;
  // The current run of the rolling hash, used to normalize the chunk size distribution by requiring
  // multiple consecutive matches to create a new chunk.
  private int nthRun;
  // Current chunk size in bytes, reset to 0 when a new chunk is created.
  private long chunkSize;

  /**
   * The chunkers of a file, one per leaf column of {@code schema}, for every row group.
   *
   * @param schema the schema of the file
   * @param options the content defined chunking options of the file
   * @return the chunkers
   */
  public static List<ContentDefinedChunker> forColumns(MessageType schema, ContentDefinedChunkingOptions options) {
    return schema.getColumns().stream()
        .map(path -> new ContentDefinedChunker(path, options))
        .collect(Collectors.toList());
  }

  ContentDefinedChunker(ColumnDescriptor path, ContentDefinedChunkingOptions options) {
    this.minChunkSize = options.getMinChunkSize();
    this.maxChunkSize = options.getMaxChunkSize();
    this.rollingHashMask = calculateMask(minChunkSize, maxChunkSize, options.getNormLevel());
    this.maxDefinitionLevel = path.getMaxDefinitionLevel();
    this.maxRepetitionLevel = path.getMaxRepetitionLevel();
    this.byteWidth = byteWidth(path.getPrimitiveType());
  }

  /** The bytes an integer value is hashed as: 1 for an INT(8), 2 for an INT(16), 4 or 8 for other integers. */
  private static int byteWidth(PrimitiveType type) {
    if (type.getLogicalTypeAnnotation() instanceof IntLogicalTypeAnnotation integer) {
      return integer.getBitWidth() / Byte.SIZE;
    }
    return switch (type.getPrimitiveTypeName()) {
      case INT32 -> Integer.BYTES;
      case INT64 -> Long.BYTES;
      default -> 0;
    };
  }

  /**
   * Calculate the mask to use for the rolling hash, the mask is used to determine if a new
   * chunk should be created based on the rolling hash value. The mask is calculated based on the
   * minChunkSize, maxChunkSize and normLevel parameters.
   *
   * <p>Assuming that the gear hash has random values with a uniform distribution, then each bit in the
   * actual value of the rolling hash has even probability of being set so a mask with the top N bits
   * set has a probability of 1/2^N of matching the rolling hash. This is the judgment criteria for the
   * original gear hash based content-defined chunking. The main drawback of this approach is the
   * non-uniform distribution of the chunk sizes.
   *
   * <p>Later on the FastCDC has improved the process by introducing:
   *
   * <ul>
   *   <li>sub-minimum chunk cut-point skipping (not hashing the first minChunkSize bytes)
   *   <li>chunk size normalization (using two masks)
   * </ul>
   *
   * <p>This implementation uses cut-point skipping because it improves the overall performance, and a
   * more accurate alternative to have a less skewed chunk size distribution. Instead of using two
   * different masks (one with a lower and one with a higher probability of matching and switching them
   * based on the actual chunk size), we rather use 8 different gear hash tables and require having 8
   * consecutive matches while switching between the used hash tables. This approach is based on the
   * central limit theorem and approximates normal distribution of the chunk sizes.
   *
   * @param minChunkSize the minimum chunk size in bytes (default 256 KiB)
   * @param maxChunkSize the maximum chunk size in bytes (default 1 MiB)
   * @param normLevel the normalization level (default 0)
   * @return the mask used to compare against the rolling hash
   * @throws IllegalArgumentException if the minimum is negative, the maximum not above it, or the
   *     mask would not have 1 to 63 bits
   */
  public static long calculateMask(long minChunkSize, long maxChunkSize, int normLevel) {
    Preconditions.checkArgument(
        minChunkSize >= 0, "Invalid content defined chunking minimum chunk size (negative): %s", minChunkSize);
    Preconditions.checkArgument(
        maxChunkSize > minChunkSize,
        "Invalid content defined chunking size range: maximum chunk size (%s) must be greater than minimum chunk size (%s)",
        maxChunkSize,
        minChunkSize);
    // Calculate the average size of the chunks, halving before adding so that the sum cannot overflow
    long avgChunkSize = minChunkSize / 2 + maxChunkSize / 2 + (minChunkSize % 2 + maxChunkSize % 2) / 2;
    // Since we are skipping the first minChunkSize bytes for each chunk, we need to target a smaller
    // chunk size to reach the average size after skipping the first minChunkSize bytes; also divide by
    // the number of gear hash tables to have a more gaussian-like distribution
    long targetSize = (avgChunkSize - minChunkSize) / GearHashTable.TABLE.length;
    // Assuming that the gear hash has a uniform distribution, we can calculate the mask by taking the
    // floor(log2(targetSize))
    int targetBits = Long.SIZE - Long.numberOfLeadingZeros(targetSize);
    int maskBits = targetBits == 0 ? 0 : targetBits - 1;
    // A user defined normLevel can be used to adjust the mask size, hence the matching probability: by
    // increasing the normLevel we increase the probability of matching the mask, forcing the
    // distribution closer to the average size
    int effectiveBits = maskBits - normLevel;

    Preconditions.checkArgument(
        effectiveBits >= 1 && effectiveBits <= 63,
        "The content defined chunking mask must be between 1 and 63 bits but was %s"
            + " (minimum chunk size %s, maximum chunk size %s, normalization level %s)",
        effectiveBits,
        minChunkSize,
        maxChunkSize,
        normLevel);
    // Create the mask by setting the top bits
    return -1L << (Long.SIZE - effectiveBits);
  }

  boolean startsNewChunk(int value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(value, byteWidth);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(long value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(value, byteWidth);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(float value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    // Raw bits, as floatToIntBits would canonicalize NaN payloads
    roll(Float.floatToRawIntBits(value), 4);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(double value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(Double.doubleToRawLongBits(value), 8);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(boolean value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    // One byte per boolean, not one bit
    roll(value ? 1 : 0, 1);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(Binary value, int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    roll(value);
    return repetitionLevel == 0 && needNewChunk();
  }

  boolean startsNewChunk(int repetitionLevel, int definitionLevel) {
    rollLevels(repetitionLevel, definitionLevel);
    return repetitionLevel == 0 && needNewChunk();
  }

  private void rollLevels(int repetitionLevel, int definitionLevel) {
    if (maxDefinitionLevel > 0) {
      roll(definitionLevel, 2);
    }
    if (maxRepetitionLevel > 0) {
      roll(repetitionLevel, 2);
    }
  }

  private void roll(long bits, int width) {
    chunkSize += width;
    if (chunkSize < minChunkSize) {
      // Short-circuit if we haven't reached the minimum chunk size, this speeds up the chunking process
      // since the gear hash doesn't need to be updated
      return;
    }
    long hash = rollingHash;
    boolean matched = hasMatched;
    long[] table = GearHashTable.TABLE[nthRun];
    for (int i = 0; i < width; ++i, bits >>>= 8) {
      hash = (hash << 1) + table[(int) bits & 0xFF];
      matched |= (hash & rollingHashMask) == 0;
    }
    rollingHash = hash;
    hasMatched = matched;
  }

  private void roll(Binary value) {
    chunkSize += value.length();
    if (chunkSize < minChunkSize) {
      // Short-circuit if we haven't reached the minimum chunk size, this speeds up the chunking process
      // since the gear hash doesn't need to be updated
      return;
    }
    // A view of the value's bytes, whether on or off the heap, without copying them
    ByteBuffer bytes = value.toByteBuffer();
    long hash = rollingHash;
    boolean matched = hasMatched;
    long[] table = GearHashTable.TABLE[nthRun];
    for (int i = bytes.position(); i < bytes.limit(); ++i) {
      hash = (hash << 1) + table[bytes.get(i) & 0xFF];
      matched |= (hash & rollingHashMask) == 0;
    }
    rollingHash = hash;
    hasMatched = matched;
  }

  /**
   * Decide whether to create a new chunk based on the rolling hash; hasMatched is set to true if we
   * encountered a match since the last needNewChunk() call.
   */
  private boolean needNewChunk() {
    if (hasMatched) {
      hasMatched = false;
      // In order to have a normal distribution of chunk sizes, we only create a new chunk if the adjusted
      // mask matches the rolling hash 8 times in a row, each run uses a different gear hash table (gear
      // hash's chunk size has geometric distribution, and we use central limit theorem to approximate
      // normal distribution, see section 6.2.1 in paper https://www.cidrdb.org/cidr2023/papers/p43-low.pdf)
      if (++nthRun >= GearHashTable.TABLE.length) {
        // Note that we choose not to reset the rolling hash state here, nor anywhere else in the code,
        // in practice this doesn't seem to affect the chunking effectiveness
        nthRun = 0;
        chunkSize = 0;
        return true;
      }
    }
    if (chunkSize >= maxChunkSize) {
      // We have a hard limit on the maximum chunk size, note that we don't reset the rolling hash state
      // here, so the next needNewChunk() call will continue from the current state
      chunkSize = 0;
      return true;
    }
    return false;
  }
}
