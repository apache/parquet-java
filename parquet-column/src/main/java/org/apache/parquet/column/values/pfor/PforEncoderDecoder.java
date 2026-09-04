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
package org.apache.parquet.column.values.pfor;

/**
 * Core PFOR encoding/decoding logic with histogram-based cost model.
 *
 * <p>The cost model selects the optimal bit width by evaluating:
 * <pre>
 * total_cost(b) = num_elements * b + num_exceptions(b) * (16 + value_bits)
 * </pre>
 * where {@code value_bits} is 32 for INT32 and 64 for INT64, and
 * {@code num_exceptions(b)} is the count of residuals requiring more than {@code b} bits.
 *
 * <p>The same model decides the delta mode: a vector is costed as it stands and
 * again as the differences between its successive values, and the cheaper of the
 * two wins. See {@link #chooseVectorPlanForInt}.
 */
public final class PforEncoderDecoder {

  private PforEncoderDecoder() {
    // Utility class
  }

  /** Result of the optimal bit width search. */
  public static final class BitWidthResult {
    public final int bitWidth;
    public final int numExceptions;
    /** What the winning width costs, in bits, under the cost model above. */
    public final long costBits;

    BitWidthResult(int bitWidth, int numExceptions, long costBits) {
      this.bitWidth = bitWidth;
      this.numExceptions = numExceptions;
      this.costBits = costBits;
    }
  }

  /**
   * How one vector is to be encoded: whether to difference it first, and the frame,
   * width and exception count that follow from that choice.
   *
   * <p>The frame and start value are held as longs for both physical types; an INT32
   * writer narrows them back to int, which is lossless because they came from ints.
   */
  public static final class VectorPlan {
    public final boolean delta;
    public final long frameOfReference;
    /** The vector's first value, meaningful only when {@link #delta} is set. */
    public final long startValue;

    public final int bitWidth;
    public final int numExceptions;
    public final long costBits;

    VectorPlan(
        boolean delta, long frameOfReference, long startValue, int bitWidth, int numExceptions, long costBits) {
      this.delta = delta;
      this.frameOfReference = frameOfReference;
      this.startValue = startValue;
      this.bitWidth = bitWidth;
      this.numExceptions = numExceptions;
      this.costBits = costBits;
    }
  }

  // Exception cost: position(16 bits) + one full-width value.
  private static final long INT32_EXCEPTION_BITS = 16 + 32;
  private static final long INT64_EXCEPTION_BITS = 16 + 64;

  /**
   * Enough of a sample to place a distribution across the width bins, and few enough
   * that the pass is a fraction of the one it is deciding against.
   */
  private static final int DELTA_SAMPLE_TARGET = 128;

  /**
   * Find the optimal bit width for packing INT32 unsigned deltas.
   *
   * <p>Builds a histogram of bits required per delta, then evaluates each
   * candidate bit width from 0 to 32 using the cost model.
   *
   * @param deltas unsigned deltas (values[] - min), treated as unsigned int
   * @param numElements number of elements
   * @return the optimal bit width and resulting number of exceptions
   */
  public static BitWidthResult findOptimalBitWidthForInt(int[] deltas, int numElements) {
    // Histogram: bitsHist[b] = count of deltas that need exactly b bits
    int[] bitsHist = new int[33]; // 0..32
    for (int i = 0; i < numElements; i++) {
      bitsHist[bitWidthForInt(deltas[i])]++;
    }

    return bestFromHistogram(bitsHist, 32, numElements, INT32_EXCEPTION_BITS);
  }

  /**
   * Find the optimal bit width for packing INT64 unsigned deltas.
   *
   * @param deltas unsigned deltas (values[] - min), treated as unsigned long
   * @param numElements number of elements
   * @return the optimal bit width and resulting number of exceptions
   */
  public static BitWidthResult findOptimalBitWidthForLong(long[] deltas, int numElements) {
    // Histogram: bitsHist[b] = count of deltas that need exactly b bits
    int[] bitsHist = new int[65]; // 0..64
    for (int i = 0; i < numElements; i++) {
      bitsHist[bitWidthForLong(deltas[i])]++;
    }

    return bestFromHistogram(bitsHist, 64, numElements, INT64_EXCEPTION_BITS);
  }

  /**
   * Walk the candidate widths over a histogram of required widths and return the
   * cheapest.
   *
   * <p>{@code bitsHist[b]} counts the residuals needing exactly {@code b} bits, so
   * the residuals needing more than {@code b} -- the exceptions at that candidate --
   * are what remains above it, and one subtraction per step keeps that count. Ties
   * keep the narrower width, since the first candidate to reach a cost wins.
   *
   * @param maxBits the physical type's width, the widest candidate
   * @param exceptionBitsPerValue what one exception costs: its position and its value
   */
  private static BitWidthResult bestFromHistogram(
      int[] bitsHist, int maxBits, int numElements, long exceptionBitsPerValue) {
    long bestCost = Long.MAX_VALUE;
    int bestBitWidth = 0;
    int bestExceptions = 0;

    // At candidate width b, residuals needing more than b bits are exceptions.
    // bitsRequired(0) is 0, so at b = 0 that is everything except the zeros.
    int exceptionsAbove = numElements - bitsHist[0];

    for (int b = 0; b <= maxBits; b++) {
      long totalCost = (long) numElements * b + (long) exceptionsAbove * exceptionBitsPerValue;
      if (totalCost < bestCost) {
        bestCost = totalCost;
        bestBitWidth = b;
        bestExceptions = exceptionsAbove;
      }
      if (b < maxBits) {
        exceptionsAbove -= bitsHist[b + 1];
      }
    }

    return new BitWidthResult(bestBitWidth, bestExceptions, bestCost);
  }

  /**
   * Decide how to encode one INT32 vector: as it stands, or as its differences.
   *
   * <p>Both transforms are costed with the same model and the cheaper one wins, so the
   * mode is a per-vector decision rather than a per-column one. It has to be: a column
   * is rarely all one shape, and differencing costs bits on any stretch whose successive
   * values are not close.
   *
   * @param values the vector
   * @param numElements element count, greater than 0
   * @param deltaScratch scratch for numElements differences. On return it holds the
   *     differences if the plan chose the delta mode, and is clobbered either way.
   * @param deltaEnabled whether the delta mode may be chosen at all
   */
  public static VectorPlan chooseVectorPlanForInt(
      int[] values, int numElements, int[] deltaScratch, boolean deltaEnabled) {
    VectorPlan raw = searchForInt(values, numElements);

    // One element has no difference to take, and a vector already packing at width 0
    // cannot be improved on.
    if (!deltaEnabled || numElements < 2 || raw.bitWidth == 0) {
      return raw;
    }

    // A delta vector carries its own first value, so it starts one full-width value
    // behind whatever its differences pack to.
    final long startValueBits = 32;

    // Estimate the mode before paying for it, and drop it here if the estimate cannot
    // reach the incumbent. What that skips is the whole of the rest of the mode: the
    // pass that writes the differences out, and the search over them. The estimate is
    // deliberately loose, so it declines only where the two modes are more than a
    // sampling error apart, which is where the choice matters least.
    if (estimateDeltaCostBitsForInt(values, numElements) + startValueBits >= raw.costBits) {
      return raw;
    }

    computeDeltasForInt(values, numElements, deltaScratch);
    VectorPlan delta = searchForInt(deltaScratch, numElements);
    long deltaCost = delta.costBits + startValueBits;
    if (deltaCost >= raw.costBits) {
      return raw;
    }
    return new VectorPlan(true, delta.frameOfReference, values[0], delta.bitWidth, delta.numExceptions, deltaCost);
  }

  /** Decide how to encode one INT64 vector. See {@link #chooseVectorPlanForInt}. */
  public static VectorPlan chooseVectorPlanForLong(
      long[] values, int numElements, long[] deltaScratch, boolean deltaEnabled) {
    VectorPlan raw = searchForLong(values, numElements);

    if (!deltaEnabled || numElements < 2 || raw.bitWidth == 0) {
      return raw;
    }

    final long startValueBits = 64;
    if (estimateDeltaCostBitsForLong(values, numElements) + startValueBits >= raw.costBits) {
      return raw;
    }

    computeDeltasForLong(values, numElements, deltaScratch);
    VectorPlan delta = searchForLong(deltaScratch, numElements);
    long deltaCost = delta.costBits + startValueBits;
    if (deltaCost >= raw.costBits) {
      return raw;
    }
    return new VectorPlan(true, delta.frameOfReference, values[0], delta.bitWidth, delta.numExceptions, deltaCost);
  }

  /**
   * Take the frame of an INT32 vector and cost the widths over it, without writing the
   * residuals out: they are needed once here, for their widths, and again by the caller
   * only once the plan is settled.
   */
  private static VectorPlan searchForInt(int[] source, int numElements) {
    int frame = source[0];
    for (int i = 1; i < numElements; i++) {
      if (source[i] < frame) {
        frame = source[i];
      }
    }

    int[] bitsHist = new int[33];
    for (int i = 0; i < numElements; i++) {
      bitsHist[bitWidthForInt(source[i] - frame)]++;
    }

    BitWidthResult best = bestFromHistogram(bitsHist, 32, numElements, INT32_EXCEPTION_BITS);
    return new VectorPlan(false, frame, 0, best.bitWidth, best.numExceptions, best.costBits);
  }

  /** See {@link #searchForInt}. */
  private static VectorPlan searchForLong(long[] source, int numElements) {
    long frame = source[0];
    for (int i = 1; i < numElements; i++) {
      if (source[i] < frame) {
        frame = source[i];
      }
    }

    int[] bitsHist = new int[65];
    for (int i = 0; i < numElements; i++) {
      bitsHist[bitWidthForLong(source[i] - frame)]++;
    }

    BitWidthResult best = bestFromHistogram(bitsHist, 64, numElements, INT64_EXCEPTION_BITS);
    return new VectorPlan(false, frame, 0, best.bitWidth, best.numExceptions, best.costBits);
  }

  /**
   * Fill {@code deltas} with the backward differences of {@code values}.
   *
   * <p>{@code deltas[0]} is 0: the first value travels in the plan's start value, and
   * giving slot 0 a real difference would mean either a shorter packed run or a value
   * that is not a difference sitting in the width histogram. Zero costs the bit width
   * and distorts nothing.
   *
   * <p>The subtraction wraps, which is what makes the round trip exact for a column
   * that spans the type's range; the reader's prefix sum wraps the same way. A vector
   * with negative differences needs nothing special: the frame is the minimum of the
   * differences, so subtracting it makes every residual non-negative, the same
   * mechanism a plain vector uses for negative values.
   */
  public static void computeDeltasForInt(int[] values, int numElements, int[] deltas) {
    deltas[0] = 0;
    for (int i = 1; i < numElements; i++) {
      deltas[i] = values[i] - values[i - 1];
    }
  }

  /** See {@link #computeDeltasForInt}. */
  public static void computeDeltasForLong(long[] values, int numElements, long[] deltas) {
    deltas[0] = 0;
    for (int i = 1; i < numElements; i++) {
      deltas[i] = values[i] - values[i - 1];
    }
  }

  /**
   * Estimate what packing the differences of an INT32 vector would cost, in bits.
   *
   * <p>The full decision needs the differences written out and searched, which is most
   * of what encoding a vector costs. This reaches an answer good enough to decline the
   * mode from a strided sample, without writing anything.
   *
   * <p>The sample is of widths, not of a span. A gate on the span of the differences
   * was tried first and had to go: a sawtooth is a tight cluster of small positive
   * differences with a handful of large negative ones, so its span is as wide as its
   * raw span while its cost is a fraction of it. Feeding widths to the same cost model
   * the search uses keeps that shape, because the model can trade a wide bin against a
   * patch.
   *
   * <p>Zigzagging is what lets a histogram stand in for a frame search that has not
   * run. Differences in [-k, k] zigzag into [0, 2k], and a frame at -k maps them onto
   * the same [0, 2k], so for a range that straddles zero evenly the estimated width is
   * the width the search would find. Where the range leans one way the estimate runs a
   * bit or two wide.
   */
  static long estimateDeltaCostBitsForInt(int[] values, int numElements) {
    int stride = Math.max(1, numElements / DELTA_SAMPLE_TARGET);
    int[] bitsHist = new int[33];
    int sampled = 0;
    for (int i = stride; i < numElements; i += stride) {
      bitsHist[bitWidthForInt(zigZagInt(values[i] - values[i - 1]))]++;
      sampled++;
    }
    if (sampled == 0) {
      return 0;
    }

    long sampleCost = bestFromHistogram(bitsHist, 32, sampled, INT32_EXCEPTION_BITS).costBits;
    // Scale to the whole vector. Both terms of the model are per element -- a width
    // costs its bits every element, an exception costs its slot every time it occurs --
    // so the sample cost scales with the count.
    return sampleCost * numElements / sampled;
  }

  /** See {@link #estimateDeltaCostBitsForInt}. */
  static long estimateDeltaCostBitsForLong(long[] values, int numElements) {
    int stride = Math.max(1, numElements / DELTA_SAMPLE_TARGET);
    int[] bitsHist = new int[65];
    int sampled = 0;
    for (int i = stride; i < numElements; i += stride) {
      bitsHist[bitWidthForLong(zigZagLong(values[i] - values[i - 1]))]++;
      sampled++;
    }
    if (sampled == 0) {
      return 0;
    }

    long sampleCost = bestFromHistogram(bitsHist, 64, sampled, INT64_EXCEPTION_BITS).costBits;
    return sampleCost * numElements / sampled;
  }

  /** Maps a signed value onto an unsigned one of about the same magnitude. */
  static int zigZagInt(int value) {
    return (value << 1) ^ (value >> 31);
  }

  /** See {@link #zigZagInt}. */
  static long zigZagLong(long value) {
    return (value << 1) ^ (value >> 63);
  }

  /**
   * Returns the number of bits required to represent an unsigned int value.
   * Returns 0 for value == 0.
   */
  public static int bitWidthForInt(int value) {
    if (value == 0) return 0;
    return 32 - Integer.numberOfLeadingZeros(value);
  }

  /**
   * Returns the number of bits required to represent an unsigned long value.
   * Returns 0 for value == 0.
   */
  public static int bitWidthForLong(long value) {
    if (value == 0) return 0;
    return 64 - Long.numberOfLeadingZeros(value);
  }
}
