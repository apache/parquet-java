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
package org.apache.parquet.column.statistics.bloomfilter;

import org.apache.commons.lang.ArrayUtils;
import org.apache.parquet.io.api.Binary;

import java.util.Arrays;
import java.util.List;

/**
 * BloomFilter is a probabilistic data structure for set membership check. BloomFilters are
 * highly space efficient when compared to using a HashSet. Because of the probabilistic nature of
 * bloom filter false positive (element not present in bloom filter but test() says true) are
 * possible but false negatives are not possible (if element is present then test() will never
 * say false). The false positive probability is configurable (default: 5%) depending on which
 * storage requirement may increase or decrease. Lower the false positive probability greater
 * is the space requirement.
 * Bloom filters are sensitive to number of elements that will be inserted in the bloom filter.
 * During the creation of bloom filter expected number of entries must be specified. If the number
 * of insertions exceed the specified initial number of entries then false positive probability will
 * increase accordingly.
 * <p/>
 * Internally, this implementation of bloom filter uses Murmur3 fast non-cryptographic hash
 * algorithm. Although Murmur2 is slightly faster than Murmur3 in Java, it suffers from hash
 * collisions for specific sequence of repeating bytes. Check the following link for more info
 * https://code.google.com/p/smhasher/wiki/MurmurHash2Flaw
 */
public class BloomFilter {
    public static final double DEFAULT_FALSE_POSITIVE_PROBABILITY = 0.05;
    protected BitSet bitSet;
    protected int numBits;
    protected int numHashFunctions;

    public BloomFilter(int numBits, int numHashFunctions) {
        this.numBits = numBits;
        this.numHashFunctions = numHashFunctions;
        this.bitSet = new BitSet(numBits);
    }

    public void add(byte[] val) {
        if (val == null) {
            addBytes(val, -1);
        } else {
            addBytes(val, val.length);
        }
    }

    public void addBytes(byte[] val, int length) {
        // We use the trick mentioned in "Less Hashing, Same Performance: Building a Better Bloom Filter"
        // by Kirsch et.al. From abstract 'only two hash functions are necessary to effectively
        // implement a Bloom filter without any loss in the asymptotic false positive probability'

        // Lets split up 64-bit hashcode into two 32-bit hash codes and employ the technique mentioned
        // in the above paper
        long hash64 = val == null ? Murmur3.NULL_HASHCODE : Murmur3.hash64(val, length);
        addHash(hash64);
    }

    private void addHash(long hash64) {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            bitSet.set(pos);
        }
    }

    public void addBinary(Binary val) {
        if (val == null) {
            add(null);
        } else {
            add(val.getBytes());
        }
    }

    public void addLong(long val) {
        addHash(getLongHash(val));
    }

    public void addDouble(double val) {
        addLong(Double.doubleToLongBits(val));
    }
    
    public void addInteger(int val){
        addLong(val);
    }

    public void addFloat(float val) {
        addLong(Float.floatToIntBits(val));
    }
    
    public boolean test(byte[] val) {
        if (val == null) {
            return testBytes(val, -1);
        }
        return testBytes(val, val.length);
    }

    public boolean testBytes(byte[] val, int length) {
        long hash64 = val == null ? Murmur3.NULL_HASHCODE : Murmur3.hash64(val, length);
        return testHash(hash64);
    }

    private boolean testHash(long hash64) {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            if (!bitSet.get(pos)) {
                return false;
            }
        }
        return true;
    }

    public boolean testBinary(Binary val) {
        if (val == null) {
            return test(null);
        } else {
            return test(val.getBytes());
        }
    }

    public boolean testLong(long val) {
        return testHash(getLongHash(val));
    }

    public boolean testFloat(float val) {
        return testInteger(Float.floatToIntBits(val));
    }
    
    public boolean testInteger(int val){
        return testLong(val);
    }

    // Thomas Wang's integer hash function
    // http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
    private long getLongHash(long key) {
        key = (~key) + (key << 21); // key = (key << 21) - key - 1;
        key = key ^ (key >> 24);
        key = (key + (key << 3)) + (key << 8); // key * 265
        key = key ^ (key >> 14);
        key = (key + (key << 2)) + (key << 4); // key * 21
        key = key ^ (key >> 28);
        key = key + (key << 31);
        return key;
    }

    public boolean testDouble(double val) {
        return testLong(Double.doubleToLongBits(val));
    }

    public long sizeInBytes() {
        return getBitSize() / 8;
    }

    public int getBitSize() {
        return bitSet.getData().length * Long.SIZE;
    }

    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    public long[] getBitSet() {
        return bitSet.getData();
    }

    public void setBitSet(long[] data) {
        this.bitSet = new BitSet(data.clone());
    }

    public int getNumBits() {
        return numBits;
    }

    @Override
    public String toString() {
        return "m: " + numBits + " k: " + numHashFunctions;
    }

    /**
     * Merge the specified bloom filter with current bloom filter.
     *
     * @param that - bloom filter to merge
     */
    public void merge(BloomFilter that) {
        if (this != that && this.numBits == that.numBits && this.numHashFunctions == that.numHashFunctions) {
            this.bitSet.putAll(that.bitSet);
        } else {
            throw new IllegalArgumentException("BloomFilters are not compatible for merging." +
                    " this - " + this.toString() + " that - " + that.toString());
        }
    }

    public void reset() {
        this.bitSet.clear();
    }

    /**
     * Bare metal bit set implementation. For performance reasons, this implementation does not check
     * for index bounds nor expand the bit set size if the specified index is greater than the size.
     */
    public class BitSet {
        private final long[] data;

        public BitSet(long bits) {
            this(new long[(int) Math.ceil((double) bits / (double) Long.SIZE)]);
        }

        /**
         * Deserialize long array as bit set.
         *
         * @param data - bit array
         */
        public BitSet(long[] data) {
            assert data.length > 0 : "data length is zero!";
            this.data = data;
        }

        /**
         * Sets the bit at specified index.
         *
         * @param index - position
         */
        public void set(int index) {
            data[index >>> 6] |= (1L << index);
        }

        /**
         * Returns true if the bit is set in the specified index.
         *
         * @param index - position
         * @return - value at the bit position
         */
        public boolean get(int index) {
            return (data[index >>> 6] & (1L << index)) != 0;
        }

        /**
         * Number of bits
         */
        public long bitSize() {
            return (long) data.length * Long.SIZE;
        }

        public long[] getData() {
            return data;
        }

        /**
         * Combines the two BitArrays using bitwise OR.
         */
        public void putAll(BitSet array) {
            assert data.length == array.data.length :
                    "BitArrays must be of equal length (" + data.length + "!= " + array.data.length + ")";
            for (int i = 0; i < data.length; i++) {
                data[i] |= array.data[i];
            }
        }

        /**
         * Clear the bit set.
         */
        public void clear() {
            Arrays.fill(data, 0);
        }
    }
}

