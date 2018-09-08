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
package org.apache.parquet.column.values.bloomfilter;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

/**
 * A Bloom filter is a compact structure to indicate whether an item is not in a set or probably
 * in a set. BloomFilter class stores a bit set represents a elements set, a hash strategy and a
 * Bloom filter algorithm.
 *
 * This Bloom filter is implemented using block-based Bloom filter algorithm from Putze et al.'s
 * "Cache-, Hash- and Space-Efficient Bloom filters". The basic idea is to hash the item to a tiny
 * Bloom filter which size fit a single cache line or smaller. This implementation sets 8 bits in
 * each tiny Bloom filter. Each tiny Bloom filter is 32 bytes to take advantage of 32-byte SIMD
 * instruction.
 */

public class BloomFilter {
  // Bloom filter Hash strategy .
  public enum HashStrategy {
    MURMUR3_X64_128,
  }

  // Bloom filter algorithm.
  public enum Algorithm {
    BLOCK,
  }

  // Bytes in a tiny Bloom filter block.
  private static final int BYTES_PER_FILTER_BLOCK = 32;

  // Default seed for hash function, it comes from System.nanoTime().
  private static final int DEFAULT_SEED = 1361930890;

  // Minimum Bloom filter size, set to size of a tiny Bloom filter block
  public static final int MINIMUM_BLOOM_FILTER_BYTES = 32;

  // Maximum Bloom filter size, it sets to default HDFS block size for upper boundary check
  // This should be re-consider when implementing write side logic.
  public static final int MAXIMUM_BLOOM_FILTER_BYTES = 128 * 1024 * 1024;

  // The number of bits to set in a tiny Bloom filter
  private static final int BITS_SET_PER_BLOCK = 8;

  // The header of Bloom filter, it includes number of bytes, algorithm and hash enumeration.
  public static final int HEADER_SIZE = 12;

  // Hash strategy used in this Bloom filter.
  public final HashStrategy hashStrategy;

  // Algorithm used in this Bloom filter.
  public final Algorithm algorithm;

  // The underlying byte array for Bloom filter bitset.
  private byte[] bitset;

  // A integer array buffer of underlying bitset to help setting bits.
  private IntBuffer intBuffer;

  // Hash function use to compute hash for column value.
  private HashFunction hashFunction;

  // The block-based algorithm needs 8 odd SALT values to calculate eight index
  // of bit to set, one bit in 32-bit word.
  private static final int SALT[] = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

  /**
   * Constructor of Bloom filter.
   *
   * @param numBytes The number of bytes for Bloom filter bitset. The range of num_bytes should be within
   *                 [MINIMUM_BLOOM_FILTER_BYTES, MAXIMUM_BLOOM_FILTER_BYTES], it will be rounded up/down
   *                 to lower/upper bound if num_bytes is out of range and also will rounded up to a power
   *                 of 2. It uses murmur3_x64_128 as its default hash function and block-based algorithm
   *                 as default algorithm.
   */
  public BloomFilter(int numBytes) {
    this(numBytes, HashStrategy.MURMUR3_X64_128, Algorithm.BLOCK);
  }

  /**
   * Constructor of Bloom filter. It uses murmur3_x64_128 as its default hash
   * function and block-based algorithm as its default algorithm.
   *
   * @param numBytes The number of bytes for Bloom filter bitset
   * @param hashStrategy The hash strategy of Bloom filter.
   * @param algorithm The algorithm of Bloom filter.
   */
  private BloomFilter(int numBytes, HashStrategy hashStrategy, Algorithm algorithm) {
    initBitset(numBytes);

    switch (hashStrategy) {
      case MURMUR3_X64_128:
        this.hashStrategy = hashStrategy;
        hashFunction = Hashing.murmur3_128(DEFAULT_SEED);
        break;
      default:
        throw new RuntimeException("Not supported hash strategy");
    }

    this.algorithm = algorithm;
  }


  /**
   * Construct the Bloom filter with given bitset, it is used when reconstructing
   * Bloom filter from parquet file. It use murmur3_x64_128 as its default hash
   * function and block-based algorithm as default algorithm.
   *
   * @param bitset The given bitset to construct Bloom filter.
   */
  public BloomFilter(byte[] bitset) {
    this(bitset, HashStrategy.MURMUR3_X64_128, Algorithm.BLOCK);
  }

  /**
   * Construct the Bloom filter with given bitset, it is used when reconstructing
   * Bloom filter from parquet file.
   *
   * @param bitset The given bitset to construct Bloom filter.
   * @param hashStrategy The hash strategy Bloom filter apply.
   * @param algorithm The algorithm of Bloom filter.
   */
  private BloomFilter(byte[] bitset, HashStrategy hashStrategy, Algorithm algorithm) {
    if (bitset == null) {
      throw new RuntimeException("Given bitset is null");
    }
    this.bitset = bitset;
    this.intBuffer = ByteBuffer.wrap(bitset).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();

    switch (hashStrategy) {
      case MURMUR3_X64_128:
        this.hashStrategy = hashStrategy;
        hashFunction = Hashing.murmur3_128(DEFAULT_SEED);
        break;
      default:
        throw new RuntimeException("Not supported hash strategy");
    }
    this.algorithm = algorithm;
  }

  /**
   * Create a new bitset for Bloom filter.
   *
   * @param numBytes The number of bytes for Bloom filter bitset. The range of num_bytes should be within
   *                 [MINIMUM_BLOOM_FILTER_BYTES, MAXIMUM_BLOOM_FILTER_BYTES], it will be rounded up/down
   *                 to lower/upper bound if num_bytes is out of range and also will rounded up to a power
   *                 of 2. It uses murmur3_x64_128 as its default hash function and block-based algorithm
   *                 as default algorithm.
   */
  private void initBitset(int numBytes) {
    if (numBytes < MINIMUM_BLOOM_FILTER_BYTES) {
      numBytes = MINIMUM_BLOOM_FILTER_BYTES;
    }

    // Get next power of 2 if it is not power of 2.
    if ((numBytes & (numBytes - 1)) != 0) {
      numBytes = Integer.highestOneBit(numBytes) << 1;
    }

    if (numBytes > MAXIMUM_BLOOM_FILTER_BYTES || numBytes < 0) {
      numBytes = MAXIMUM_BLOOM_FILTER_BYTES;
    }

    this.bitset = new byte[numBytes];
    this.intBuffer = ByteBuffer.wrap(bitset).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
  }

  /**
   * Write the Bloom filter to an output stream. It writes the Bloom filter header includes the
   * bitset's length in size of byte, the hash strategy, the algorithm, and the bitset.
   *
   * @param out the output stream to write
   */
  public void writeTo(OutputStream out) throws IOException {
    // Write number of bytes of bitset.
    out.write(BytesUtils.intToBytes(bitset.length));

    // Write hash strategy
    out.write(BytesUtils.intToBytes(this.hashStrategy.ordinal()));

    // Write algorithm
    out.write(BytesUtils.intToBytes(this.algorithm.ordinal()));

    // Write bitset
    out.write(bitset);
  }

  private int[] setMask(int key) {
    int mask[] = new int[BITS_SET_PER_BLOCK];

    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
      mask[i] = key * SALT[i];
    }

    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
      mask[i] = mask[i] >>> 27;
    }

    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
      mask[i] = 0x1 << mask[i];
    }

    return mask;
  }

  /**
   * Add an element to Bloom filter, the element content is represented by
   * the hash value of its plain encoding result.
   *
   * @param hash the hash result of element.
   */
  public void insert(long hash) {
    int bucketIndex = (int)(hash >> 32) & (bitset.length / BYTES_PER_FILTER_BLOCK - 1);
    int key = (int)hash;

    // Calculate mask for bucket.
    int mask[] = setMask(key);

    for (int i = 0; i < BITS_SET_PER_BLOCK; i++) {
      int value = intBuffer.get(bucketIndex * (BYTES_PER_FILTER_BLOCK / 4) + i);
      value |= mask[i];
      intBuffer.put(bucketIndex * (BYTES_PER_FILTER_BLOCK / 4) + i, value);
    }
  }

  /**
   * Determine whether an element is in set or not.
   *
   * @param hash the hash value of element plain encoding result.
   * @return false if element is must not in set, true if element probably in set.
   */
  public boolean find(long hash) {
    int bucketIndex = (int)(hash >> 32) & (bitset.length / BYTES_PER_FILTER_BLOCK - 1);
    int key = (int)hash;

    // Calculate mask for the tiny Bloom filter.
    int mask[] = setMask(key);

    for (int i = 0; i < BITS_SET_PER_BLOCK; i++) {
      if (0 == (intBuffer.get(bucketIndex * (BYTES_PER_FILTER_BLOCK / 4) + i) & mask[i])) {
        return false;
      }
    }

    return true;
  }

  /**
   * Calculate optimal size according to the number of distinct values and false positive probability.
   *
   * @param n: The number of distinct values.
   * @param p: The false positive probability.
   * @return optimal number of bits of given n and p.
   */
  public static int optimalNumOfBits(long n, double p) {
    Preconditions.checkArgument((p > 0.0 && p < 1.0),
      "FPP should be less than 1.0 and great than 0.0");

    final double m = -8 * n / Math.log(1 - Math.pow(p, 1.0 / 8));
    final double MAX = MAXIMUM_BLOOM_FILTER_BYTES << 3;
    int numBits = (int)m;

    // Handle overflow.
    if (m > MAX || m < 0) {
      numBits = (int)MAX;
    }

    // Get next power of 2 if bits is not power of 2.
    if ((numBits & (numBits - 1)) != 0) {
      numBits = Integer.highestOneBit(numBits) << 1;
    }

    if (numBits < (MINIMUM_BLOOM_FILTER_BYTES << 3)) {
      numBits = MINIMUM_BLOOM_FILTER_BYTES << 3;
    }

    return numBits;
  }

  /**
   * Compute hash for int value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  public long hash(int value) {
    ByteBuffer plain = ByteBuffer.allocate(Integer.SIZE/Byte.SIZE);
    plain.order(ByteOrder.LITTLE_ENDIAN).putInt(value);
    return hashFunction.hashBytes(plain.array()).asLong();
  }

  /**
   * Compute hash for long value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  public long hash(long value) {
    ByteBuffer plain = ByteBuffer.allocate(Long.SIZE/Byte.SIZE);
    plain.order(ByteOrder.LITTLE_ENDIAN).putLong(value);
    return hashFunction.hashBytes(plain.array()).asLong();
  }

  /**
   * Compute hash for double value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  public long hash(double value) {
    ByteBuffer plain = ByteBuffer.allocate(Double.SIZE/Byte.SIZE);
    plain.order(ByteOrder.LITTLE_ENDIAN).putDouble(value);
    return hashFunction.hashBytes(plain.array()).asLong();
  }

  /**
   * Compute hash for float value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  public long hash(float value) {
    ByteBuffer plain = ByteBuffer.allocate(Float.SIZE/Byte.SIZE);
    plain.order(ByteOrder.LITTLE_ENDIAN).putFloat(value);
    return hashFunction.hashBytes(plain.array()).asLong();
  }

  /**
   * Compute hash for Binary value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  public long hash(Binary value) {
      return hashFunction.hashBytes(value.toByteBuffer()).asLong();
  }

  /**
   * Get allocated buffer size.
   * @return size in byte.
   */
  public long getBufferedSize() {
    return this.bitset.length;
  }
}
