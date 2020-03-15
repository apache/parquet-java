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

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Arrays;

/*
 * This Bloom filter is implemented using block-based Bloom filter algorithm from Putze et al.'s
 * "Cache-, Hash- and Space-Efficient Bloom filters". The basic idea is to hash the item to a tiny
 * Bloom filter which size fit a single cache line or smaller. This implementation sets 8 bits in
 * each tiny Bloom filter. Each tiny Bloom filter is 32 bytes to take advantage of 32-byte SIMD
 * instruction.
 */
public class BlockSplitBloomFilter implements BloomFilter {
  // Bytes in a tiny Bloom filter block.
  private static final int BYTES_PER_BLOCK = 32;

  // Bits in a tiny Bloom filter block.
  private static final int BITS_PER_BLOCK = 256;

  // Default minimum Bloom filter size, set to the size of a tiny Bloom filter block
  public static final int DEFAULT_MINIMUM_BYTES = 32;

  // Default Maximum Bloom filter size, set to 1MB which should cover most cases.
  public static final int DEFAULT_MAXIMUM_BYTES = 1024 * 1024;

  // The number of bits to set in a tiny Bloom filter
  private static final int BITS_SET_PER_BLOCK = 8;

  // The metadata in the header of a serialized Bloom filter is four four-byte values: the number of bytes,
  // the filter algorithm, the hash algorithm, and the compression.
  public static final int HEADER_SIZE = 16;

  // The default false positive probability value
  public static final double DEFAULT_FPP = 0.01;

  // Hash strategy used in this Bloom filter.
  private final HashStrategy hashStrategy;

  // The underlying byte array for Bloom filter bitset.
  private byte[] bitset;

  // A integer array buffer of underlying bitset to help setting bits.
  private IntBuffer intBuffer;

  // Hash function use to compute hash for column value.
  private HashFunction hashFunction;

  private int maximumBytes = DEFAULT_MAXIMUM_BYTES;
  private int minimumBytes = DEFAULT_MINIMUM_BYTES;

  // The block-based algorithm needs 8 odd SALT values to calculate eight indexes
  // of bits to set, one per 32-bit word.
  private static final int[] SALT = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

  /**
   * Constructor of block-based Bloom filter.
   *
   * @param numBytes The number of bytes for Bloom filter bitset. The range of num_bytes should be within
   *                 [DEFAULT_MINIMUM_BYTES, DEFAULT_MAXIMUM_BYTES], it will be rounded up/down
   *                 to lower/upper bound if num_bytes is out of range. It will also be rounded up to a power
   *                 of 2. It uses XXH64 as its default hash function.
   */
  public BlockSplitBloomFilter(int numBytes) {
    this(numBytes, DEFAULT_MAXIMUM_BYTES, HashStrategy.XXH64);
  }

  /**
   * Constructor of block-based Bloom filter.
   *
   * @param numBytes The number of bytes for Bloom filter bitset. The range of num_bytes should be within
   *                 [DEFAULT_MINIMUM_BYTES, DEFAULT_MAXIMUM_BYTES], it will be rounded up/down
   *                 to lower/upper bound if num_bytes is out of range. It will also be rounded up to a power
   *                 of 2. It uses XXH64 as its default hash function.
   * @param maximumBytes The maximum bytes of the Bloom filter.
   */
  public BlockSplitBloomFilter(int numBytes, int maximumBytes) {
    this(numBytes, maximumBytes, HashStrategy.XXH64);
  }

  /**
   * Constructor of block-based Bloom filter.
   *
   * @param numBytes The number of bytes for Bloom filter bitset
   * @param hashStrategy The hash strategy of Bloom filter.
   */
  private BlockSplitBloomFilter(int numBytes, HashStrategy hashStrategy) {
    this(numBytes, DEFAULT_MAXIMUM_BYTES, hashStrategy);
  }

  /**
   * Constructor of block-based Bloom filter.
   *
   * @param numBytes The number of bytes for Bloom filter bitset. The range of num_bytes should be within
   *                 [DEFAULT_MINIMUM_BYTES, maximumBytes], it will be rounded up/down to lower/upper bound if
   *                 num_bytes is out of range. It will also be rounded up to a power of 2.
   * @param maximumBytes The maximum bytes of the Bloom filter.
   * @param hashStrategy The adopted hash strategy of the Bloom filter.
   */
  public BlockSplitBloomFilter(int numBytes, int maximumBytes, HashStrategy hashStrategy) {
    if (maximumBytes > DEFAULT_MINIMUM_BYTES) {
      this.maximumBytes = maximumBytes;
    }
    initBitset(numBytes);

    switch (hashStrategy) {
      case XXH64:
        this.hashStrategy = hashStrategy;
        hashFunction = new XxHash();
        break;
      default:
        throw new RuntimeException("Unsupported hash strategy");
    }
  }


  /**
   * Construct the Bloom filter with given bitset, it is used when reconstructing
   * Bloom filter from parquet file. It use XXH64 as its default hash
   * function.
   *
   * @param bitset The given bitset to construct Bloom filter.
   */
  public BlockSplitBloomFilter(byte[] bitset) {
    this(bitset, HashStrategy.XXH64);
  }

  /**
   * Construct the Bloom filter with given bitset, it is used when reconstructing
   * Bloom filter from parquet file.
   *
   * @param bitset The given bitset to construct Bloom filter.
   * @param hashStrategy The hash strategy Bloom filter apply.
   */
  private BlockSplitBloomFilter(byte[] bitset, HashStrategy hashStrategy) {
    if (bitset == null) {
      throw new RuntimeException("Given bitset is null");
    }

    this.bitset = bitset;
    this.intBuffer = ByteBuffer.wrap(bitset).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    switch (hashStrategy) {
      case XXH64:
        this.hashStrategy = hashStrategy;
        hashFunction = new XxHash();
        break;
      default:
        throw new RuntimeException("Unsupported hash strategy");
    }
  }

  /**
   * Create a new bitset for Bloom filter.
   *
   * @param numBytes The number of bytes for Bloom filter bitset. The range of num_bytes should be within
   *                 [minimumBytes, maximumBytes], it will be rounded up/down
   *                 to lower/upper bound if num_bytes is out of range and also will rounded up to a power
   *                 of 2. It uses XXH64 as its default hash function and block-based algorithm
   *                 as default algorithm.
   */
  private void initBitset(int numBytes) {
    if (numBytes < minimumBytes) {
      numBytes = minimumBytes;
    }
    // Get next power of 2 if it is not power of 2.
    if ((numBytes & (numBytes - 1)) != 0) {
      numBytes = Integer.highestOneBit(numBytes) << 1;
    }
    if (numBytes > maximumBytes || numBytes < 0) {
      numBytes = maximumBytes;
    }
    this.bitset = new byte[numBytes];
    this.intBuffer = ByteBuffer.wrap(bitset).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    // Write number of bytes of bitset.
    out.write(BytesUtils.intToBytes(bitset.length));
    // Write hash strategy
    out.write(BytesUtils.intToBytes(hashStrategy.value));
    // Write algorithm
    out.write(BytesUtils.intToBytes(Algorithm.BLOCK.value));
    // Write compression
    out.write(BytesUtils.intToBytes(Compression.UNCOMPRESSED.value));
    // Write bitset
    out.write(bitset);
  }

  private int[] setMask(int key) {
    int[] mask = new int[BITS_SET_PER_BLOCK];

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

  @Override
  public void insertHash(long hash) {
    long numBlocks = bitset.length / BYTES_PER_BLOCK;
    long lowHash = hash >>> 32;
    int blockIndex = (int)((lowHash * numBlocks) >> 32);
    int key = (int)hash;

    // Calculate mask for bucket.
    int[] mask = setMask(key);
    for (int i = 0; i < BITS_SET_PER_BLOCK; i++) {
      int value = intBuffer.get(blockIndex * (BYTES_PER_BLOCK / 4) + i);
      value |= mask[i];
      intBuffer.put(blockIndex * (BYTES_PER_BLOCK / 4) + i, value);
    }
  }

  @Override
  public boolean findHash(long hash) {
    long numBlocks = bitset.length / BYTES_PER_BLOCK;
    long lowHash = hash >>> 32;
    int blockIndex = (int)((lowHash * numBlocks) >> 32);
    int key = (int)hash;

    // Calculate mask for the tiny Bloom filter.
    int[] mask = setMask(key);
    for (int i = 0; i < BITS_SET_PER_BLOCK; i++) {
      if (0 == (intBuffer.get(blockIndex * (BYTES_PER_BLOCK / 4) + i) & mask[i])) {
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
    final double MAX = DEFAULT_MAXIMUM_BYTES << 3;
    int numBits = (int)m;

    // Handle overflow.
    if (m > MAX || m < 0) {
      numBits = (int)MAX;
    }

    // Round to BITS_PER_BLOCK
    numBits = (numBits + BITS_PER_BLOCK -1) & ~BITS_PER_BLOCK;

    if (numBits < (DEFAULT_MINIMUM_BYTES << 3)) {
      numBits = DEFAULT_MINIMUM_BYTES << 3;
    }

    return numBits;
  }

  @Override
  public long getBitsetSize() {
    return this.bitset.length;
  }

  @Override
  public long hash(Object value) {
    ByteBuffer plain;

    if (value instanceof Binary) {
      return hashFunction.hashBytes(((Binary) value).getBytes());
    }

    if (value instanceof Integer) {
      plain = ByteBuffer.allocate(Integer.SIZE/Byte.SIZE);
      plain.order(ByteOrder.LITTLE_ENDIAN).putInt(((Integer)value));
    } else if (value instanceof Long) {
      plain = ByteBuffer.allocate(Long.SIZE/Byte.SIZE);
      plain.order(ByteOrder.LITTLE_ENDIAN).putLong(((Long)value));
    } else if (value instanceof Float) {
      plain = ByteBuffer.allocate(Float.SIZE/Byte.SIZE);
      plain.order(ByteOrder.LITTLE_ENDIAN).putFloat(((Float)value));
    } else if (value instanceof Double) {
      plain = ByteBuffer.allocate(Double.SIZE/ Byte.SIZE);
      plain.order(ByteOrder.LITTLE_ENDIAN).putDouble(((Double)value));
    } else {
      throw new RuntimeException("Parquet Bloom filter: Not supported type");
    }

    return hashFunction.hashByteBuffer(plain);
  }

  @Override
  public long hash(int value) {
    ByteBuffer plain = ByteBuffer.allocate(Integer.SIZE/Byte.SIZE);
    plain.order(ByteOrder.LITTLE_ENDIAN).putInt(value);
    return hashFunction.hashByteBuffer(plain);
  }

  @Override
  public long hash(long value) {
    ByteBuffer plain = ByteBuffer.allocate(Long.SIZE/Byte.SIZE);
    plain.order(ByteOrder.LITTLE_ENDIAN).putLong(value);
    return hashFunction.hashByteBuffer(plain);
  }

  @Override
  public long hash(double value) {
    ByteBuffer plain = ByteBuffer.allocate(Double.SIZE/Byte.SIZE);
    plain.order(ByteOrder.LITTLE_ENDIAN).putDouble(value);
    return hashFunction.hashByteBuffer(plain);
  }

  @Override
  public long hash(float value) {
    ByteBuffer plain = ByteBuffer.allocate(Float.SIZE/Byte.SIZE);
    plain.order(ByteOrder.LITTLE_ENDIAN).putFloat(value);
    return hashFunction.hashByteBuffer(plain);
  }

  @Override
  public long hash(Binary value) {
    return hashFunction.hashBytes(value.getBytes());
  }

  @Override
  public boolean equals(Object object) {
    if (object == this) {
      return true;
    }
    if (object instanceof BlockSplitBloomFilter) {
      BlockSplitBloomFilter that = (BlockSplitBloomFilter) object;
      return Arrays.equals(this.bitset, that.bitset)
        && this.maximumBytes == that.maximumBytes
        && this.hashStrategy == that.hashStrategy;
    }
    return false;
  }

  private boolean checkCompatibility(BlockSplitBloomFilter that) {
    return this.maximumBytes == that.maximumBytes && this.hashStrategy == that.hashStrategy;
  }

  @Override
  public BloomFilter union(BloomFilter bloomFilter) {
    if (bloomFilter instanceof BlockSplitBloomFilter) {
      BlockSplitBloomFilter that = (BlockSplitBloomFilter) bloomFilter;

      if (!checkCompatibility(that)) {
        throw new RuntimeException("Bloom filters are not compatible!");
      }

      int numBytes = this.bitset.length;
      byte[] newBitset = new byte[numBytes];

      for (int index = 0; index < numBytes; index++) {
        newBitset[index] = (byte) (this.bitset[index] | that.bitset[index]);
      }

      return new BlockSplitBloomFilter(newBitset);

    } else {
      throw new RuntimeException("Bloom filter should be BlockSplitFilter");
    }

  }
}
