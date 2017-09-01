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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.*;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;


/**
 * Bloom Filter is a compat structure to indicate whether an item is not in set or probably in set. Bloom class is
 * underlying class of Bloom Filter which stores a bit set represents elements set, hash strategy and bloom filter
 * algorithm.
 *
 * Bloom Filter algorithm is implemented using block Bloom filters from Putze et al.'s "Cache-, Hash- and Space-Efficient Bloom
 * Filters". The basic idea is to hash the item to a tiny Bloom Filter which size fit a single cache line or smaller.
 * This implementation sets 8 bits in each tiny Bloom Filter. Tiny bloom filter are 32 bytes to take advantage of 32-bytes
 * SIMD instruction.
 *
 */

public abstract class Bloom<T extends Comparable<T>> {
  public enum HASH {
    MURMUR3_X64_128,
  }

  public enum ALGORITHM {
    BLOCK,
  }

  public final double FPP = 0.01;
  public static final int BLOOM_HEADER_SIZE = 12;
  public static final int BYTES_PER_BUCKET = 32;
  public static final int MINIMUM_BLOOM_SIZE = 256;

  public HASH bloomFilterHash = HASH.MURMUR3_X64_128;
  public ALGORITHM bloomFilterAlgorithm = ALGORITHM.BLOCK;

  private HashSet<Long> elements = new HashSet<>();

  private byte[] bitset;
  private int numBytes;

  /**
   * Create bitset immediately if numBytes is not zero.
   * @param numBytes
   */
  public Bloom(int numBytes, HASH hash, ALGORITHM algorithm) {
    if (numBytes != 0) {
      initBitset(numBytes);
    }
    this.bloomFilterHash = hash;
    this.bloomFilterAlgorithm = algorithm;
  }

  /**
   * Construct the bloom filter with given bit set.
   * @param bitset given bitset
   */
  public Bloom(byte[] bitset, HASH hash, ALGORITHM algorithm) {
    this.bitset = bitset;
    this.numBytes = bitset.length;
    this.bloomFilterHash = hash;
    this.bloomFilterAlgorithm = algorithm;
  }

  /**
   * Construct the bloom filter with given bit set.
   * @param input given bitset
   */
  public Bloom(ByteBuffer input, HASH hash, ALGORITHM algorithm) {
    if (input.isDirect()){
      bitset = new byte[input.remaining()];
      input.get(bitset);
    } else {
      this.bitset = input.array();
    }
    this.numBytes = input.array().length;
    this.bloomFilterHash = hash;
    this.bloomFilterAlgorithm = algorithm;
  }

  /**
   * Create a new bit set for bloom filter, at least 256 bits will be create.
   * @param numBytes number of bytes for bit set.
   */
  public void initBitset(int numBytes) {
    Preconditions.checkArgument((numBytes & (numBytes-1)) == 0,
      "Bloom Filter size should be power of 2");
    if (numBytes < MINIMUM_BLOOM_SIZE) numBytes = MINIMUM_BLOOM_SIZE;
    if (numBytes > ParquetProperties.DEFAULT_MAXIMUM_BLOOM_FILTER_SIZE)
      numBytes = ParquetProperties.DEFAULT_MAXIMUM_BLOOM_FILTER_SIZE;

    // wrap to 4 bytes align.
    numBytes |= 0x11;

    ByteBuffer bytes = ByteBuffer.allocate(numBytes).order(ByteOrder.BIG_ENDIAN);
    this.bitset = bytes.array();
    this.numBytes = numBytes;
  }

  /**
   * Create bitset from a given byte array.
   * @param bitset given bitset.
   */
  public void initBitset(byte[] bitset) {
    this.bitset = bitset;
    this.numBytes = bitset.length;
  }

  /**
   * Get size of elements in buffer which stores in hash map.
   */
  public long getBufferedSize() {
    return bitset == null ? elements.size() * 8 : elements.size() * 8 + numBytes;
  }

  /**
   * Get bitset size.
   * @return size of bitset
   */
  public long getBloomSize() {
    return numBytes;
  }

  public long getAllocatedSize() {
    return getBufferedSize();
  }


  public BytesInput getBytes() throws IOException {
    // TODO: move out of here and reuse
    List<BytesInput> inputs= new ArrayList<>(4);

    // number of bytes.
    inputs.add(BytesInput.fromInt(numBytes));

    // hash
    inputs.add(BytesInput.fromInt(this.bloomFilterHash.ordinal()));

    // algorithm
    inputs.add(BytesInput.fromInt(this.bloomFilterAlgorithm.ordinal()));

    // bitset.
    inputs.add(BytesInput.from(bitset, 0, bitset.length));

    return BytesInput.concat(inputs);
  }

  /**
   * Add an element which represent by hash to bloom bitset.
   * @param hash hash result of element.
   */
  public void bloomInsert(long hash) {
    // The size of one bucket is set to 32 bytes.
    int idx = (int)(hash >> 32) & (numBytes / BYTES_PER_BUCKET - 1);
    int key = (int)hash;

    int mask[] = new int[BYTES_PER_BUCKET/4];
    setMask(key, mask);

    byte[] bucket = new byte[BYTES_PER_BUCKET];

    IntBuffer intBuffer = ByteBuffer.wrap(bucket).
      order(ByteOrder.BIG_ENDIAN).asIntBuffer();

    intBuffer.put(mask);

    for (int i = 0; i < BYTES_PER_BUCKET; ++i) {
      bitset[BYTES_PER_BUCKET * idx + i] |= bucket[i];
    }
  }

  /**
   * Build bloom filter bitset according existing elements in buffer.
   */
  public void flush() {
    if (bitset == null) {
      initBitset(optimalNumOfBits(elements.size(), FPP)/8);
      for (long hash : elements) {
        bloomInsert(hash);
      }
      elements.clear();
    }
  }

  /**
   * Calculate optimal size according to the number of distinct values and false positive probability.
   * @param n: The number of distinct values.
   * @param p: The false positive probability.
   * @return optimal number of bits of given n and p.
   */
  public static int optimalNumOfBits(long n, double p) {
    int bits = (int)(-n * Math.log(p) / (Math.log(2) * Math.log(2)));

    bits --;
    bits |= bits >> 1;
    bits |= bits >> 2;
    bits |= bits >> 4;
    bits |= bits >> 8;
    bits |= bits >> 16;
    bits++;

    return bits;
  }


  private int[] setMask(int key, int mask[]) {
    final int SALT[] = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

    for (int i = 0; i < 8; ++i) {
      mask[i] = key * SALT[i];
    }

    for (int i = 0; i < 8; ++i) {
      mask[i] = mask[i] >> 27;
    }

    for (int i = 0; i < 8; ++i) {
      mask[i] = 0x1 << mask[i];
    }
    return mask;
  }

  /**
   * Determine where an element is must not in set or probably in set.
   * @param hash represent element
   * @return false if element is must not in set, true if element probably in set.
   */
  public boolean bloomFind(long hash) {
    int idx = (int)(hash >> 32) & (numBytes / 32 - 1);
    int key = (int)hash;
    int mask[] = new int[8];
    setMask(key, mask);

    IntBuffer intBuffer = ByteBuffer.wrap(bitset, 32 * idx, 32).
      order(ByteOrder.BIG_ENDIAN).asIntBuffer();

    int[] intset = new int[8];
    intBuffer.get(intset);

    for (int i = 0; i < 8; ++i) {
      if (0 == (intset[i] & mask[i]))
        return false;
    }

    return true;

  }

  /**
   * Element is represented by hash in bloom filter. The hash function takes plain encoding of element as input.
   */
  public Encoding getEncoding() {
    return Encoding.PLAIN;
  }

  public String memUsageString(String prefix) {
    return String.format(
      "%s BloomDataWriter{\n" + "%s}\n",
      prefix, String.valueOf(bitset.length)
    );
  }

  public static Bloom getBloomOnType(PrimitiveType.PrimitiveTypeName type,
                                     int size,
                                     HASH hash,
                                     ALGORITHM algorithm) {
    switch(type) {
      case INT32:
        return new IntBloom(size, hash, algorithm);
      case INT64:
        return new LongBloom(size, hash, algorithm);
      case FLOAT:
        return new FloatBloom(size, hash, algorithm);
      case DOUBLE:
        return new DoubleBloom(size, hash, algorithm);
      case BINARY:
        return new BinaryBloom(size, hash, algorithm);
      case INT96:
        return new BinaryBloom(size, hash, algorithm);
      case FIXED_LEN_BYTE_ARRAY:
        return new BinaryBloom(size, hash, algorithm);
      default:
        return null;
    }
  }

  // TODO: add doc for public member and function.

  /**
   * Compute hash for values' plain encoding result.
   * @param value the column value to be compute
   * @return hash result
   */
  public abstract long hash(T value);

  /**
   * Insert element to set represented by bloom bitset.
   * @param value the column value to be inserted.
   */
  public void insert(T value) {
    if(bitset != null) {
      bloomInsert(hash(value));
    } else {
      elements.add(hash(value));
    }
  }

  /**
   * Determine whether an element exist in set or not.
   * @param value the element to find.
   * @return false if value is definitely not in set, and true means PROBABLY in set.
   */
  public boolean find (T value) {
    return bloomFind(hash(value));
  }

  public static class BinaryBloom extends Bloom<Binary> {
    private CapacityByteArrayOutputStream arrayout = new CapacityByteArrayOutputStream(1024, 64 * 1024, new HeapByteBufferAllocator());
    private LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(arrayout);

    public BinaryBloom(int size) {
      super(size, HASH.MURMUR3_X64_128, ALGORITHM.BLOCK);
    }

    public BinaryBloom(int size, HASH hash, ALGORITHM algorithm) {
      super(size, hash, algorithm);
    }

    @Override
    public long hash(Binary value) {
      try {
        out.writeInt(value.length());
        value.writeTo(out);
        out.flush();
        byte[] encoded = BytesInput.from(arrayout).toByteArray();
        arrayout.reset();
        switch (bloomFilterHash) {
          case MURMUR3_X64_128: return Murmur3.hash64(encoded);
          default:
            throw new RuntimeException("Not support hash strategy");
        }
      } catch (IOException e) {
        throw new ParquetEncodingException("could not insert Binary value to bloom ", e);
      }
    }
  }


  public static class LongBloom extends Bloom<Long> {
    public LongBloom(int size) {
      super(size, HASH.MURMUR3_X64_128, ALGORITHM.BLOCK);
    }

    public LongBloom (int size, HASH hash, ALGORITHM algorithm) {
      super(size, hash, algorithm);
    }

    @Override
    public long hash(Long value) {
      ByteBuffer plain = ByteBuffer.allocate(Long.SIZE/Byte.SIZE);
      plain.order(ByteOrder.LITTLE_ENDIAN).putLong(value);
      switch (bloomFilterHash) {
        case MURMUR3_X64_128: return Murmur3.hash64(plain.array());
        default:
          throw new RuntimeException("Not support hash strategy");
      }
    }
  }

  public static class IntBloom extends Bloom<Integer> {
    public IntBloom(int size) {
      super(size, HASH.MURMUR3_X64_128, ALGORITHM.BLOCK);
    }

    public IntBloom(int size, HASH hash, ALGORITHM algorithm) {
      super(size, hash, algorithm);
    }

    @Override
    public long hash(Integer value) {
      ByteBuffer plain = ByteBuffer.allocate(Integer.SIZE/Byte.SIZE);
      plain.order(ByteOrder.LITTLE_ENDIAN).putInt(value);
      switch (bloomFilterHash) {
        case MURMUR3_X64_128: return Murmur3.hash64(plain.array());
        default:
          throw new RuntimeException("Not support hash strategy");
      }
    }
  }

  public static class FloatBloom extends Bloom<Float> {
    public FloatBloom(int size) {
      super(size, HASH.MURMUR3_X64_128, ALGORITHM.BLOCK);
    }

    public FloatBloom(int size, HASH hash, ALGORITHM algorithm) {
      super(size, hash, algorithm);
    }

    @Override
    public long hash(Float value) {
      ByteBuffer plain = ByteBuffer.allocate(Float.SIZE/Byte.SIZE);
      plain.order(ByteOrder.LITTLE_ENDIAN).putFloat(value);
      switch (bloomFilterHash) {
        case MURMUR3_X64_128: return Murmur3.hash64(plain.array());
        default:
          throw new RuntimeException("Not support hash strategy");
      }
    }
  }

  public static class DoubleBloom extends Bloom<Double> {
    public DoubleBloom(int size) {
      super(size, HASH.MURMUR3_X64_128, ALGORITHM.BLOCK);
    }

    public DoubleBloom(int size, HASH hash, ALGORITHM algorithm) {
      super(size, hash, algorithm);
    }

    @Override
    public long hash(Double value) {
      ByteBuffer plain = ByteBuffer.allocate(Double.SIZE/Byte.SIZE);
      plain.order(ByteOrder.LITTLE_ENDIAN).putDouble(value);
      switch (bloomFilterHash) {
        case MURMUR3_X64_128: return Murmur3.hash64(plain.array());
        default:
          throw new RuntimeException("Not support hash strategy");
      }
    }
  }
}