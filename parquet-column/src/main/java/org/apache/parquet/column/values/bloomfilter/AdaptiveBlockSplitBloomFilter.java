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

import static org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter.LOWER_BOUND_BYTES;
import static org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter.UPPER_BOUND_BYTES;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;

/**
 * The purpose of this is to finally generate a bloom filter with the optimal bit size according to the number
 * of real data distinct values.
 * `AdaptiveBlockSplitBloomFilter` contains multiple `BlockSplitBloomFilter` as candidates and inserts values in
 * the candidates at the same time. Some candidates that are too small will be eliminated during the insertion process.
 * Finally we will choose the most appropriate size candidate to write out.
 */
public class AdaptiveBlockSplitBloomFilter implements BloomFilter {

  private static final Logger LOG = LoggerFactory.getLogger(AdaptiveBlockSplitBloomFilter.class);

  // multiple candidates, inserting data at the same time. If the distinct values are greater than the
  // expected NDV of one candidate, it will be removed. Finally we will choose the smallest candidate to write out.
  private final List<BloomFilterCandidate> candidates = new ArrayList<>();

  // the largest among candidates and also used as an approximate deduplication counter
  private BloomFilterCandidate largestCandidate;

  // the accumulator of the number of distinct hash values that have been inserted so far
  private long numDistinctHashValues = 0;

  // indicates that the bloom filter candidate has been written out and new data should be no longer allowed to be inserted
  private boolean finalized = false;

  // indicates the step size to find the NDV value corresponding to numBytes
  private static final int NDV_STEP = 500;
  private int maximumBytes = UPPER_BOUND_BYTES;
  private int minimumBytes = LOWER_BOUND_BYTES;
  private int minimumCandidateNdv = 16;
  // the hash strategy used in this bloom filter.
  private final HashStrategy hashStrategy;
  // the column to build bloom filter
  private ColumnDescriptor column;

  /**
   * @param maximumBytes  the maximum bytes size of candidate
   * @param numCandidates the number of candidates
   * @param fpp           the false positive probability
   */
  public AdaptiveBlockSplitBloomFilter(int maximumBytes, int numCandidates, double fpp, ColumnDescriptor column) {
    this(maximumBytes, HashStrategy.XXH64, fpp, numCandidates, column);
  }

  public AdaptiveBlockSplitBloomFilter(int maximumBytes, HashStrategy hashStrategy, double fpp,
    int numCandidates, ColumnDescriptor column) {
    this.column = column;
    switch (hashStrategy) {
      case XXH64:
        this.hashStrategy = hashStrategy;
        break;
      default:
        throw new RuntimeException("Unsupported hash strategy");
    }
    initCandidates(maximumBytes, numCandidates, fpp);
  }

  /**
   * This method will generate candidates according to the maximum acceptable bytes size of bloom filter.
   * Because the bytes size of the candidate need to be a power of 2, here we set the candidate size to be
   * a proportion of `maxBytes` like `1/2`, `1/4`, `1/8`, etc.
   *
   * @param maxBytes      the maximum bytes size of candidate
   * @param numCandidates the number of candidates
   * @param fpp           the false positive probability
   */
  private void initCandidates(int maxBytes, int numCandidates, double fpp) {
    int candidateByteSize = calculateBoundedPowerOfTwo(maxBytes);
    for (int i = 0; i < numCandidates; i++) {
      int candidateExpectedNDV = expectedNDV(candidateByteSize, fpp);
      // `candidateByteSize` is too small, just drop it
      if (candidateExpectedNDV <= 0) {
        break;
      }
      BloomFilterCandidate candidate =
        new BloomFilterCandidate(candidateExpectedNDV, candidateByteSize, minimumBytes, maximumBytes, hashStrategy);
      candidates.add(candidate);
      candidateByteSize = calculateBoundedPowerOfTwo(candidateByteSize / 2);
    }
    if (candidates.isEmpty()) {
      // `maxBytes` is too small, but at least one candidate will be generated, 32 bytes size and can accept 16 distinct values.
      candidates.add(new BloomFilterCandidate(minimumCandidateNdv, minimumBytes, minimumBytes, maximumBytes, hashStrategy));
    }
    largestCandidate = candidates.stream().max(BloomFilterCandidate::compareTo).get();
  }

  /**
   * According to the size of bytes, calculate the expected number of distinct values.
   * The expected number result may be slightly smaller than what `numBytes` can support.
   *
   * @param numBytes the bytes size
   * @param fpp      the false positive probability
   * @return the expected number of distinct values
   */
  private int expectedNDV(int numBytes, double fpp) {
    int expectedNDV = 0;
    int optimalBytes = 0;
    while (optimalBytes < numBytes) {
      expectedNDV += NDV_STEP;
      optimalBytes = BlockSplitBloomFilter.optimalNumOfBits(expectedNDV, fpp) / 8;
    }
    // make sure it is slightly smaller than what `numBytes` can support
    expectedNDV -= NDV_STEP;
    // numBytes is too small
    if (expectedNDV <= 0) {
      expectedNDV = 0;
    }
    return expectedNDV;
  }

  /**
   * BloomFilter bytes size should be power of 2, see [[BlockSplitBloomFilter#initBitset]]
   *
   * @param numBytes the bytes size
   * @return the largest power of 2 less or equal to numBytes
   */
  private int calculateBoundedPowerOfTwo(int numBytes) {
    if (numBytes < minimumBytes) {
      numBytes = minimumBytes;
    }
    // if `numBytes` is not power of 2, get the largest power of two less than `numBytes`
    if ((numBytes & (numBytes - 1)) != 0) {
      numBytes = Integer.highestOneBit(numBytes);
    }
    numBytes = Math.min(numBytes, maximumBytes);
    numBytes = Math.max(numBytes, minimumBytes);
    return numBytes;
  }

  /**
   * Used at the end of the insertion, select the candidate of the smallest size.
   * At least one of the largest candidates will be kept when inserting data.
   *
   * @return the smallest and optimal candidate
   */
  protected BloomFilterCandidate optimalCandidate() {
    return candidates.stream().min(BloomFilterCandidate::compareTo).get();
  }

  protected List<BloomFilterCandidate> getCandidates() {
    return candidates;
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    finalized = true;
    BloomFilterCandidate optimalBloomFilter = optimalCandidate();
    optimalBloomFilter.bloomFilter.writeTo(out);
    String columnName = column != null && column.getPath() != null ? Arrays.toString(column.getPath()) : "unknown";
    LOG.info("The number of distinct values in {} is approximately {}, the optimal bloom filter can accept {}"
        + " distinct values, byte size is {}.",
      columnName, numDistinctHashValues, optimalBloomFilter.getExpectedNDV(),
      optimalBloomFilter.bloomFilter.getBitsetSize());
  }

  /**
   * Insert an element to the multiple bloom filter candidates and remove the bad candidate
   * if the number of distinct values exceeds its expected size.
   *
   * @param hash the hash result of element.
   */
  @Override
  public void insertHash(long hash) {
    Preconditions.checkArgument(!finalized,
      "Insertion has been mark as finalized, no more data is allowed!");
    if (!largestCandidate.bloomFilter.findHash(hash)) {
      numDistinctHashValues++;
    }
    // distinct values exceed the expected size, remove the bad bloom filter (leave at least the max bloom filter candidate)
    candidates.removeIf(candidate -> candidate.getExpectedNDV() < numDistinctHashValues && candidate != largestCandidate);
    candidates.forEach(candidate -> candidate.getBloomFilter().insertHash(hash));
  }

  @Override
  public int getBitsetSize() {
    return optimalCandidate().getBloomFilter().getBitsetSize();
  }

  @Override
  public boolean findHash(long hash) {
    return largestCandidate.bloomFilter.findHash(hash);
  }

  @Override
  public long hash(Object value) {
    return largestCandidate.bloomFilter.hash(value);
  }

  @Override
  public HashStrategy getHashStrategy() {
    return largestCandidate.bloomFilter.getHashStrategy();
  }

  @Override
  public Algorithm getAlgorithm() {
    return largestCandidate.bloomFilter.getAlgorithm();
  }

  @Override
  public Compression getCompression() {
    return largestCandidate.bloomFilter.getCompression();
  }

  @Override
  public long hash(int value) {
    return largestCandidate.bloomFilter.hash(value);
  }

  @Override
  public long hash(long value) {
    return largestCandidate.bloomFilter.hash(value);
  }

  @Override
  public long hash(double value) {
    return largestCandidate.bloomFilter.hash(value);
  }

  @Override
  public long hash(float value) {
    return largestCandidate.bloomFilter.hash(value);
  }

  @Override
  public long hash(Binary value) {
    return largestCandidate.bloomFilter.hash(value);
  }

  protected class BloomFilterCandidate implements Comparable<BloomFilterCandidate> {
    // the bloom filter candidate
    final private BlockSplitBloomFilter bloomFilter;
    // the max excepted number of distinct value in the candidate
    final private int expectedNDV;

    public BloomFilterCandidate(int expectedNDV, int candidateBytes,
      int minimumBytes, int maximumBytes, HashStrategy hashStrategy) {
      this.bloomFilter = new BlockSplitBloomFilter(candidateBytes, minimumBytes, maximumBytes, hashStrategy);
      this.expectedNDV = expectedNDV;
    }

    public BlockSplitBloomFilter getBloomFilter() {
      return bloomFilter;
    }

    public int getExpectedNDV() {
      return expectedNDV;
    }

    @Override
    public int compareTo(BloomFilterCandidate o) {
      return this.bloomFilter.getBitsetSize() - o.bloomFilter.getBitsetSize();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BloomFilterCandidate that = (BloomFilterCandidate) o;
      return expectedNDV == that.expectedNDV &&
        Objects.equals(bloomFilter, that.bloomFilter);
    }

    @Override
    public int hashCode() {
      return Objects.hash(bloomFilter, expectedNDV);
    }
  }
}
