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
 *
 * <p>It also decides the frame of reference, which is any lower bound on the vector
 * rather than its minimum: a window placed where the values cluster can be narrower
 * than one anchored at an outlier below them, and the values it leaves out are patched
 * like any other exception. See {@link #searchForInt}.
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
   * Bucket count the frame search works at, as a shift and as a count. 256 buckets keep
   * the window scan in {@link #scanFrameWindow} at about two passes' worth of work over a
   * 1024-value vector.
   */
  private static final int FRAME_SEARCH_BITS = 8;

  private static final int FRAME_SEARCH_BUCKETS = 1 << FRAME_SEARCH_BITS;

  /** A run of frame search buckets: the offsets in {@code [start << shift, end << shift)}. */
  private static final class FrameWindow {
    final int start;
    final int end;

    FrameWindow(int start, int end) {
      this.start = start;
      this.end = end;
    }
  }

  /**
   * Choose a frame of reference for an INT32 vector and the width that suits it.
   *
   * <p>The frame PFOR has always used is the minimum, which makes every exception an
   * overshoot: one value far below the cluster drags the whole packed window down with it
   * and nothing can patch it back. Treating the frame as a free parameter instead -- any
   * lower bound, not the lowest -- lets the window sit where the values actually are and
   * patch on both sides. A value below the frame wraps, in the modular subtraction the
   * writer already does, to a huge offset that fails the same unsigned width test as a
   * value above the window, so it becomes an exception and is patched back with its
   * unreduced value. There is no sign or direction to track.
   *
   * <p>Nothing on the wire changes: the frame field already holds a full-width value and
   * a reader only ever adds it. The whole cost is this search, which is why a reader that
   * predates it still reads what this writer produces.
   *
   * <p>The search is approximate by design. An exact answer needs the values sorted;
   * instead the range is bucketed with a shift and for each candidate width a window is
   * slid over the bucket counts. Only whole buckets count as covered, so the exception
   * estimate is an upper bound, never optimistic. The minimum as a frame is always among
   * the candidates and it alone is costed from a real histogram, so the search can never
   * do worse than the width search alone would have.
   */
  private static VectorPlan searchForInt(int[] source, int numElements) {
    int min = source[0];
    int max = source[0];
    for (int i = 1; i < numElements; i++) {
      if (source[i] < min) {
        min = source[i];
      } else if (source[i] > max) {
        max = source[i];
      }
    }

    // The range is an unsigned quantity even though its ends are signed: it spans the
    // whole type when they sit at the extremes, and the subtraction wraps to say so.
    int range = max - min;
    if (range == 0) {
      // A constant vector is already at the floor and min/max has just proved it
      // constant. Worth its own exit for more than the saved pass: a run of equal values
      // sends every element to one histogram bin, where the read-modify-write serializes.
      return new VectorPlan(false, min, 0, 0, 0, 0);
    }

    int rangeBits = bitWidthForInt(range);
    int shift = rangeBits > FRAME_SEARCH_BITS ? rangeBits - FRAME_SEARCH_BITS : 0;

    // One walk serves both halves of the search: the width histogram costs the minimum
    // as a frame, the bucket counts cost every other frame. They are gathered together
    // because each needs the same offset.
    int[] bitsHist = new int[33];
    int[] counts = new int[FRAME_SEARCH_BUCKETS + 1];
    for (int i = 0; i < numElements; i++) {
      int offset = source[i] - min;
      bitsHist[bitWidthForInt(offset)]++;
      counts[offset >>> shift]++;
    }

    // Candidate 0: the minimum, which is what PFOR has always done. Costed
    // unconditionally, and from a real histogram, so the search cannot regress here.
    BitWidthResult best = bestFromHistogram(bitsHist, 32, numElements, INT32_EXCEPTION_BITS);
    VectorPlan minPlan = new VectorPlan(false, min, 0, best.bitWidth, best.numExceptions, best.costBits);

    // Already at width 0, with a handful of patches carrying the rest, and nothing a
    // frame can do about that. This is not the same as having no exceptions: trading a
    // narrower width for a few patches is the whole point of a frame above the minimum,
    // so an exception-free choice is where the search starts, not a reason to skip it.
    if (best.bitWidth == 0) {
      return minPlan;
    }

    int numBuckets = (range >>> shift) + 1;
    FrameWindow window =
        scanFrameWindow(counts, numBuckets, shift, 32, numElements, INT32_EXCEPTION_BITS, best.costBits);
    if (window == null) {
      return minPlan;
    }

    // Lower the frame from the boundary of the winning window onto the smallest value the
    // window actually covers. Bucket boundaries stand 2^shift apart, which on a wide
    // column is thousands, and a cluster sitting just above one would otherwise pay those
    // bits for nothing.
    //
    // A walk of its own, rather than per-bucket minima kept by the pass above: tracking
    // them there costs every vector a compare and a store per element, including the
    // vectors where the scan finds nothing and the minima are thrown away. Here only a
    // vector whose search has already won pays, and it pays one traversal.
    int windowLo = window.start << shift;
    // A window reaching the last bucket has no upper edge to test against: that edge
    // would be numBuckets << shift, one past the range whenever the offsets span the
    // whole type.
    boolean boundedAbove = window.end < numBuckets;
    int windowHi = boundedAbove ? window.end << shift : 0;

    int frameOffset = 0;
    boolean coversAnything = false;
    for (int i = 0; i < numElements; i++) {
      int offset = source[i] - min;
      if (Integer.compareUnsigned(offset, windowLo) < 0
          || (boundedAbove && Integer.compareUnsigned(offset, windowHi) >= 0)) {
        continue;
      }
      if (!coversAnything || Integer.compareUnsigned(offset, frameOffset) < 0) {
        frameOffset = offset;
        coversAnything = true;
      }
    }
    if (!coversAnything || frameOffset == 0) {
      return minPlan;
    }

    // Cost the winning frame exactly. This pass is not bookkeeping -- it is where the
    // width and the exception count are decided. The scan works at bucket granularity and
    // so cannot see a window narrower than one bucket, which is exactly where the answers
    // worth having tend to be: a sawtooth spanning 12 bits has buckets 16 wide, and no
    // scan over them resolves the 0-bit window its few patches leave behind.
    int scanFrame = min + frameOffset;
    int[] exactHist = new int[33];
    for (int i = 0; i < numElements; i++) {
      exactHist[bitWidthForInt(source[i] - scanFrame)]++;
    }
    BitWidthResult exact = bestFromHistogram(exactHist, 32, numElements, INT32_EXCEPTION_BITS);
    if (exact.costBits >= best.costBits) {
      return minPlan;
    }
    return new VectorPlan(false, scanFrame, 0, exact.bitWidth, exact.numExceptions, exact.costBits);
  }

  /** See {@link #searchForInt}. */
  private static VectorPlan searchForLong(long[] source, int numElements) {
    long min = source[0];
    long max = source[0];
    for (int i = 1; i < numElements; i++) {
      if (source[i] < min) {
        min = source[i];
      } else if (source[i] > max) {
        max = source[i];
      }
    }

    long range = max - min;
    if (range == 0) {
      return new VectorPlan(false, min, 0, 0, 0, 0);
    }

    int rangeBits = bitWidthForLong(range);
    int shift = rangeBits > FRAME_SEARCH_BITS ? rangeBits - FRAME_SEARCH_BITS : 0;

    int[] bitsHist = new int[65];
    int[] counts = new int[FRAME_SEARCH_BUCKETS + 1];
    for (int i = 0; i < numElements; i++) {
      long offset = source[i] - min;
      bitsHist[bitWidthForLong(offset)]++;
      counts[(int) (offset >>> shift)]++;
    }

    BitWidthResult best = bestFromHistogram(bitsHist, 64, numElements, INT64_EXCEPTION_BITS);
    VectorPlan minPlan = new VectorPlan(false, min, 0, best.bitWidth, best.numExceptions, best.costBits);
    if (best.bitWidth == 0) {
      return minPlan;
    }

    int numBuckets = (int) (range >>> shift) + 1;
    FrameWindow window =
        scanFrameWindow(counts, numBuckets, shift, 64, numElements, INT64_EXCEPTION_BITS, best.costBits);
    if (window == null) {
      return minPlan;
    }

    long windowLo = (long) window.start << shift;
    boolean boundedAbove = window.end < numBuckets;
    long windowHi = boundedAbove ? (long) window.end << shift : 0;

    long frameOffset = 0;
    boolean coversAnything = false;
    for (int i = 0; i < numElements; i++) {
      long offset = source[i] - min;
      if (Long.compareUnsigned(offset, windowLo) < 0
          || (boundedAbove && Long.compareUnsigned(offset, windowHi) >= 0)) {
        continue;
      }
      if (!coversAnything || Long.compareUnsigned(offset, frameOffset) < 0) {
        frameOffset = offset;
        coversAnything = true;
      }
    }
    if (!coversAnything || frameOffset == 0) {
      return minPlan;
    }

    long scanFrame = min + frameOffset;
    int[] exactHist = new int[65];
    for (int i = 0; i < numElements; i++) {
      exactHist[bitWidthForLong(source[i] - scanFrame)]++;
    }
    BitWidthResult exact = bestFromHistogram(exactHist, 64, numElements, INT64_EXCEPTION_BITS);
    if (exact.costBits >= best.costBits) {
      return minPlan;
    }
    return new VectorPlan(false, scanFrame, 0, exact.bitWidth, exact.numExceptions, exact.costBits);
  }

  /**
   * Slide a window of {@code 2^w} offsets over the bucket counts, for each candidate
   * width in turn, and return where it costs least.
   *
   * <p>Widths below the bucket size cannot be resolved at this granularity, and once one
   * window spans every bucket there are no exceptions left to remove, so only the
   * {@link #FRAME_SEARCH_BITS} or so widths in between are scanned: fixed work, and none
   * of it touching the data again.
   *
   * <p>What comes out is a frame, not a width. Only whole buckets count as covered, so
   * {@code w} here is an upper bound on the width the frame really needs and the
   * exception count an upper bound too; the caller's exact pass is what turns the frame
   * into a plan.
   *
   * @param incumbentCost the cost to beat, so a window only registers if it beats the
   *     minimum as a frame. That skips the rest of the search entirely on a column the
   *     frame cannot help, which is most of them, and it errs in the conservative
   *     direction: the scan over-counts exceptions, so it can decline a frame whose exact
   *     cost would have won, but it cannot accept one that loses.
   * @return the winning window, or null if none beat {@code incumbentCost}
   */
  private static FrameWindow scanFrameWindow(
      int[] counts,
      int numBuckets,
      int shift,
      int maxBits,
      int numElements,
      long exceptionBitsPerValue,
      long incumbentCost) {
    int[] prefix = new int[numBuckets + 1];
    for (int b = 0; b < numBuckets; b++) {
      prefix[b + 1] = prefix[b] + counts[b];
    }

    int bestStart = -1;
    int bestEnd = 0;
    long bestCost = incumbentCost;
    for (int w = shift; w <= maxBits; w++) {
      // Buckets that fit under a width of w. The exponent stays small -- there are at
      // most FRAME_SEARCH_BUCKETS buckets, so the loop leaves as soon as w - shift
      // reaches FRAME_SEARCH_BITS -- but it is clamped rather than shifted, because a
      // shift count of 64 or more is not a shift at all in Java.
      int k = (w - shift) >= FRAME_SEARCH_BITS ? numBuckets : (int) Math.min(1L << (w - shift), numBuckets);
      for (int s = 0; s < numBuckets; s++) {
        int end = Math.min(s + k, numBuckets);
        long exceptions = numElements - (prefix[end] - prefix[s]);
        long cost = (long) numElements * w + exceptions * exceptionBitsPerValue;
        if (cost < bestCost) {
          bestCost = cost;
          bestStart = s;
          bestEnd = end;
        }
      }
      if (k >= numBuckets) {
        break; // one window already spans the data
      }
    }

    return bestStart < 0 ? null : new FrameWindow(bestStart, bestEnd);
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
   * with negative differences needs nothing special: the frame is a lower bound on the
   * differences, so subtracting it leaves residuals the packed width can hold, the same
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
