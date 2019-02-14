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

import org.apache.parquet.io.api.Binary;

public abstract class BloomFilterUtility<T extends Comparable<T>> {
  public BloomFilter bloomFilter;
  public abstract boolean contains(T value);

  public static class IntBloomFilter extends BloomFilterUtility<Integer> {
    public IntBloomFilter(BloomFilter bloomFilter) {
      this.bloomFilter = bloomFilter;
    }

    @Override
    public boolean contains(Integer value) {
      return bloomFilter.findHash(bloomFilter.hash(value.intValue()));
    }
  }

  public static class LongBloomFilter extends BloomFilterUtility<Long> {
    public LongBloomFilter(BloomFilter bloomFilter) {
      this.bloomFilter = bloomFilter;
    }

    @Override
    public boolean contains(Long value) {
      return bloomFilter.findHash(bloomFilter.hash(value.longValue()));
    }
  }

  public static class DoubleBloomFilter extends BloomFilterUtility<Double> {
    public DoubleBloomFilter(BloomFilter bloomFilter) {
      this.bloomFilter = bloomFilter;
    }

    @Override
    public boolean contains(Double value) {
      return bloomFilter.findHash(bloomFilter.hash(value.doubleValue()));
    }
  }

  public static class FloatBloomFilter extends BloomFilterUtility<Float> {
    public FloatBloomFilter(BloomFilter bloomFilter) {
      this.bloomFilter = bloomFilter;
    }

    @Override
    public boolean contains(Float value) {
      return bloomFilter.findHash(bloomFilter.hash(value.floatValue()));
    }
  }

  public static class BinaryBloomFilter extends BloomFilterUtility<Binary> {
    public BinaryBloomFilter(BloomFilter bloomFilter) {
      this.bloomFilter = bloomFilter;
    }

    @Override
    public boolean contains(Binary value) {
      return bloomFilter.findHash(bloomFilter.hash(value));
    }
  }
}
