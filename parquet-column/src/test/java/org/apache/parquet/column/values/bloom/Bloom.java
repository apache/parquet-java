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
package org.apache.parquet.column.values.bloom;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.*;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.io.api.Binary;

/**
 * Bloom Filter is a compact structure to indicate whether an item is not in set or probably
 * in set. Bloom class is underlying class of Bloom Filter which stores a bit set represents
 * elements set, hash strategy and bloom filter algorithm.
 *
 * Bloom Filter algorithm is implemented using block Bloom filters from Putze et al.'s "Cache-,
 * Hash- and Space-Efficient Bloom Filters". The basic idea is to hash the item to a tiny Bloom
 * Filter which size fit a single cache line or smaller. This implementation sets 8 bits in
 * each tiny Bloom Filter. Tiny bloom filter are 32 bytes to take advantage of 32-bytes SIMD
 * instruction.
 */

public class Bloom {
  // Hash strategy available for bloom filter.
  public enum HashStrategy {
    MURMUR3_X64_128,
  }

  // Bloom filter algorithm.
  public enum Algorithm {
    BLOCK,
  }

  /**
   * Default false positive probability value use to calculate optimal number of bits
   * used by bloom filter.
   */
  public final double DEFAULT_FPP = 0.01;

  // Bloom filter data header, including number of bytes, hash strategy and algorithm.
  public static final int HEADER_SIZE = 12;

  // Bytes in a tiny bloom filter block.
  public static final int BYTES_PER_FILTER_BLOCK = 32;

  // Default seed for hash function
  public static final int DEFAULT_SEED = 104729;

  // Hash strategy used in this bloom filter.
  public final HashStrategy hashStrategy;

  // Algorithm applied of this bloom filter.
  public final Algorithm algorithm;

  // The underlying byte array for bloom filter bitset.
  private byte[] bitset;

  // A integer array buffer of underlying bitset help setting bits.
  private IntBuffer intBuffer;

  // Hash function use to compute hash for column value.
  private HashFunction hashFunction;

  // The block based algorithm needs 8 odd SALT values to calculate eight index
  // of bit to set, one bit in 32-bit word.
  private static final int SALT[] = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

  /**
   * Constructor of bloom filter, if numBytes is zero, bloom filter bitset
   * will be created lazily and the number of bytes will be calculated through
   * distinct values in cache. It use murmur3_x64_128 as its default hash function
   * and block based algorithm as default algorithm.
   * @param numBytes The number of bytes for bloom filter bitset, set to zero can
   *                 let it calculate number automatically by using default DEFAULT_FPP.
   */
  public Bloom(int numBytes) {
    this(numBytes, HashStrategy.MURMUR3_X64_128, Algorithm.BLOCK);
  }

  /**
   * Constructor of bloom filter, if numBytes is zero, bloom filter bitset
   * will be created lazily and the number of bytes will be calculated through
   * distinct values in cache.
   * @param numBytes The number of bytes for bloom filter bitset, set to zero can
   *                 let it calculate number automatically by using default DEFAULT_FPP.
   * @param hashStrategy The hash strategy bloom filter apply.
   * @param algorithm The algorithm of bloom filter.
   */
  private Bloom(int numBytes, HashStrategy hashStrategy, Algorithm algorithm) {
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
   * Construct the bloom filter with given bit set, it is used when reconstruct
   * bloom filter from parquet file.It use murmur3_x64_128 as its default hash
   * function and block based algorithm as default algorithm.
   * @param bitset The given bitset to construct bloom filter.
   */
  public Bloom(byte[] bitset) {
    this(bitset, HashStrategy.MURMUR3_X64_128, Algorithm.BLOCK);
  }

  /**
   * Construct the bloom filter with given bit set, it is used
   * when reconstruct bloom filter from parquet file.
   * @param bitset The given bitset to construct bloom filter.
   * @param hashStrategy The hash strategy bloom filter apply.
   * @param algorithm The algorithm of bloom filter.
   */
  private Bloom(byte[] bitset, HashStrategy hashStrategy, Algorithm algorithm) {
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
   * Create a new bitset for bloom filter, at least 256 bits will be create.
   * @param numBytes number of bytes for bit set.
   */
  private void initBitset(int numBytes) {
    if (numBytes < BYTES_PER_FILTER_BLOCK) {
      numBytes = BYTES_PER_FILTER_BLOCK;
    }

    // Get next power of 2 if it is not power of 2.
    if ((numBytes & (numBytes - 1)) != 0) {
      numBytes = Integer.highestOneBit(numBytes) << 1;
    }

    if (numBytes > ParquetProperties.DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES || numBytes < 0) {
      numBytes = ParquetProperties.DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES;
    }

    this.bitset = new byte[numBytes];
    this.intBuffer = ByteBuffer.wrap(bitset).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
  }

  /**
   * Write bloom filter to output stream. A bloom filter structure should include
   * bitset length, hash strategy, algorithm, and bitset.
   * @param out output stream to write
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
    int mask[] = new int[8];

    for (int i = 0; i < 8; ++i) {
      mask[i] = key * SALT[i];
    }

    for (int i = 0; i < 8; ++i) {
      mask[i] = mask[i] >>> 27;
    }

    for (int i = 0; i < 8; ++i) {
      mask[i] = 0x1 << mask[i];
    }

    return mask;
  }

  /**
   * Add an element to bloom filter, the element content is represented by
   * the hash value of its plain encoding result.
   * @param hash the hash result of element.
   */
  private void addElement(long hash) {
    int bucketIndex = (int)(hash >> 32) & (bitset.length / BYTES_PER_FILTER_BLOCK - 1);
    int key = (int)hash;

    // Calculate mask for bucket.
    int mask[] = setMask(key);

    for (int i = 0; i < 8; i++) {
      int value = intBuffer.get(bucketIndex * (BYTES_PER_FILTER_BLOCK / 4) + i);
      value |= mask[i];
      intBuffer.put(bucketIndex * (BYTES_PER_FILTER_BLOCK / 4) + i, value);
    }
  }

  /**
   * Determine where an element is in set or not.
   * @param hash the hash value of element plain encoding result.
   * @return false if element is must not in set, true if element probably in set.
   */
  private boolean contains(long hash) {
    int bucketIndex = (int)(hash >> 32) & (bitset.length / BYTES_PER_FILTER_BLOCK - 1);
    int key = (int)hash;

    // Calculate mask for bucket.
    int mask[] = setMask(key);

    for (int i = 0; i < 8; i++) {
      if (0 == (intBuffer.get(bucketIndex * (BYTES_PER_FILTER_BLOCK / 4) + i) & mask[i])) {
        return false;
      }
    }

    return true;
  }

  /**
   * Calculate optimal size according to the number of distinct values and false positive probability.
   * @param n: The number of distinct values.
   * @param p: The false positive probability.
   * @return optimal number of bits of given n and p.
   */
  public static int optimalNumOfBits(long n, double p) {
    Preconditions.checkArgument((p > 0.0 && p < 1.0),
      "FPP should be less than 1.0 and great than 0.0");

    final double M = -8 * n / Math.log(1 - Math.pow(p, 1.0 / 8));
    final double MAX = ParquetProperties.DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES << 3;
    int numBits = (int)M;

    // Handle overflow.
    if (M > MAX || M < 0) {
      numBits = (int)MAX;
    }

    // Get next power of 2 if bits is not power of 2.
    if ((numBits & (numBits - 1)) != 0) {
      numBits = Integer.highestOneBit(numBits) << 1;
    }

    // Minimum
    if (numBits < (BYTES_PER_FILTER_BLOCK << 3)) {
      numBits = BYTES_PER_FILTER_BLOCK << 3;
    }

    return numBits;
  }

  /**
   * used to decide if we want to work to the next page
   * @return Bytes buffered of bloom filter.
   */
  public long getBufferedSize() {
    return bitset.length;
  }

  /**
   * Compute hash for int value by using its plain encoding result.
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
   * @param value the value to hash
   * @return hash result
   */
  public long hash(Binary value) {
      return hashFunction.hashBytes(value.toByteBuffer()).asLong();
  }

  /**
   * Insert element to set represented by bloom bitset.
   * @param hash the hash of value to insert into bloom filter..
   */
  public void insert(long hash) {
      addElement(hash);
  }

  /**
   * Determine whether an element exist in set or not.
   * @param hash the element to contain.
   * @return false if value is definitely not in set, and true means PROBABLY in set.
   */
  public boolean find(long hash) {
    return contains(hash);
  }
}
