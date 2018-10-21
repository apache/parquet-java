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
package org.apache.parquet.internal.column.columnindex;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder.ColumnIndexBase;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/**
 * Tests the operator implementations in {@link BoundaryOrder}.
 */
public class TestBoundaryOrder {
  private static class SpyValueComparatorBuilder extends ColumnIndexBase<Integer> {
    class SpyValueComparator extends ValueComparator {
      private final ColumnIndexBase<?>.ValueComparator delegate;
      private int compareCount;

      SpyValueComparator(ColumnIndexBase<?>.ValueComparator delegate) {
        this.delegate = delegate;
      }

      int getCompareCount() {
        return compareCount;
      }

      @Override
      int arrayLength() {
        return delegate.arrayLength();
      }

      @Override
      int translate(int arrayIndex) {
        return delegate.translate(arrayIndex);
      }

      @Override
      int compareValueToMin(int arrayIndex) {
        ++compareCount;
        return delegate.compareValueToMin(arrayIndex);
      }

      @Override
      int compareValueToMax(int arrayIndex) {
        ++compareCount;
        return delegate.compareValueToMax(arrayIndex);
      }
    }

    private SpyValueComparatorBuilder() {
      super(TYPE);
    }

    SpyValueComparator build(ColumnIndexBase<?>.ValueComparator comparator) {
      return new SpyValueComparator(comparator);
    }

    @Override
    ByteBuffer getMinValueAsBytes(int arrayIndex) {
      throw new Error("Shall never be invoked");
    }

    @Override
    ByteBuffer getMaxValueAsBytes(int arrayIndex) {
      throw new Error("Shall never be invoked");
    }

    @Override
    String getMinValueAsString(int arrayIndex) {
      throw new Error("Shall never be invoked");
    }

    @Override
    String getMaxValueAsString(int arrayIndex) {
      throw new Error("Shall never be invoked");
    }

    @Override
    <T extends Comparable<T>> org.apache.parquet.filter2.predicate.Statistics<T> createStats(int arrayIndex) {
      throw new Error("Shall never be invoked");
    }

    @Override
    ColumnIndexBase<Integer>.ValueComparator createValueComparator(Object value) {
      throw new Error("Shall never be invoked");
    }
  }

  private static class ExecStats {
    private long linearTime;
    private long binaryTime;
    private int linearCompareCount;
    private int binaryCompareCount;
    private int execCount;

    IntList measureLinear(Function<ColumnIndexBase<?>.ValueComparator, PrimitiveIterator.OfInt> op,
        ColumnIndexBase<?>.ValueComparator comparator) {
      IntList list = new IntArrayList(comparator.arrayLength());
      SpyValueComparatorBuilder.SpyValueComparator spyComparator = SPY_COMPARATOR_BUILDER.build(comparator);
      long start = System.nanoTime();
      op.apply(spyComparator).forEachRemaining((int value) -> list.add(value));
      linearTime = System.nanoTime() - start;
      linearCompareCount += spyComparator.getCompareCount();
      return list;
    }

    IntList measureBinary(Function<ColumnIndexBase<?>.ValueComparator, PrimitiveIterator.OfInt> op,
        ColumnIndexBase<?>.ValueComparator comparator) {
      IntList list = new IntArrayList(comparator.arrayLength());
      SpyValueComparatorBuilder.SpyValueComparator spyComparator = SPY_COMPARATOR_BUILDER.build(comparator);
      long start = System.nanoTime();
      op.apply(spyComparator).forEachRemaining((int value) -> list.add(value));
      binaryTime = System.nanoTime() - start;
      binaryCompareCount += spyComparator.getCompareCount();
      return list;
    }

    void add(ExecStats stats) {
      linearTime += stats.linearTime;
      linearCompareCount += stats.linearCompareCount;
      binaryTime += stats.binaryTime;
      binaryCompareCount += stats.binaryCompareCount;
      ++execCount;
    }

    @Override
    public String toString() {
      double linearMs = linearTime / 1_000_000.0;
      double binaryMs = binaryTime / 1_000_000.0;
      return String.format(
          "Linear search: %.2fms (avg: %.6fms); number of compares: %d (avg: %d) [100.00%%]%n"
              + "Binary search: %.2fms (avg: %.6fms); number of compares: %d (avg: %d) [%.2f%%]",
          linearMs, linearMs / execCount, linearCompareCount, linearCompareCount / execCount,
          binaryMs, binaryMs / execCount, binaryCompareCount, binaryCompareCount / execCount,
          100.0 * binaryCompareCount / linearCompareCount);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TestBoundaryOrder.class);
  private static final PrimitiveType TYPE = Types.required(PrimitiveTypeName.INT32).named("test_int32");
  private static final int FROM = -15;
  private static final int TO = 15;
  private static final ColumnIndexBase<?> ASCENDING;
  private static final ColumnIndexBase<?> DESCENDING;
  private static final int SINGLE_FROM = -1;
  private static final int SINGLE_TO = 1;
  private static final ColumnIndexBase<?> SINGLE;
  private static final Random RANDOM = new Random(42);
  private static final int RAND_FROM = -2000;
  private static final int RAND_TO = 2000;
  private static final int RAND_COUNT = 2000;
  private static final ColumnIndexBase<?> RAND_ASCENDING;
  private static final ColumnIndexBase<?> RAND_DESCENDING;
  private static final SpyValueComparatorBuilder SPY_COMPARATOR_BUILDER = new SpyValueComparatorBuilder();
  static {
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(TYPE, Integer.MAX_VALUE);
    builder.add(stats(FROM, -12));
    builder.add(stats(-10, -8));
    builder.add(stats(-8, -4));
    builder.add(stats(-6, -4));
    builder.add(stats(-6, -3));
    builder.add(stats(-6, -3));
    builder.add(stats(-6, -3));
    builder.add(stats(0, 3));
    builder.add(stats(3, 5));
    builder.add(stats(3, 5));
    builder.add(stats(5, 8));
    builder.add(stats(10, TO));
    ASCENDING = (ColumnIndexBase<?>) builder.build();

    builder = ColumnIndexBuilder.getBuilder(TYPE, Integer.MAX_VALUE);
    builder.add(stats(10, TO));
    builder.add(stats(5, 8));
    builder.add(stats(3, 5));
    builder.add(stats(3, 5));
    builder.add(stats(0, 3));
    builder.add(stats(-6, -3));
    builder.add(stats(-6, -3));
    builder.add(stats(-6, -3));
    builder.add(stats(-6, -4));
    builder.add(stats(-8, -4));
    builder.add(stats(-10, -8));
    builder.add(stats(FROM, -12));
    DESCENDING = (ColumnIndexBase<?>) builder.build();

    builder = ColumnIndexBuilder.getBuilder(TYPE, Integer.MAX_VALUE);
    builder.add(stats(SINGLE_FROM, SINGLE_TO));
    SINGLE = (ColumnIndexBase<?>) builder.build();

    builder = ColumnIndexBuilder.getBuilder(TYPE, Integer.MAX_VALUE);
    for (PrimitiveIterator.OfInt it = IntStream.generate(() -> RANDOM.nextInt(RAND_TO - RAND_FROM + 1) + RAND_FROM)
        .limit(RAND_COUNT * 2).sorted().iterator(); it.hasNext();) {
      builder.add(stats(it.nextInt(), it.nextInt()));
    }
    RAND_ASCENDING = (ColumnIndexBase<?>) builder.build();

    builder = ColumnIndexBuilder.getBuilder(TYPE, Integer.MAX_VALUE);
    for (Iterator<Integer> it = IntStream.generate(() -> RANDOM.nextInt(RAND_TO - RAND_FROM + 1) + RAND_FROM)
        .limit(RAND_COUNT * 2).mapToObj(Integer::valueOf).sorted(Collections.reverseOrder()).iterator(); it
            .hasNext();) {
      builder.add(stats(it.next(), it.next()));
    }
    RAND_DESCENDING = (ColumnIndexBase<?>) builder.build();
  }

  private static Statistics<?> stats(int min, int max) {
    Statistics<?> stats = Statistics.createStats(TYPE);
    stats.updateStats(min);
    stats.updateStats(max);
    return stats;
  }

  private static ExecStats validateOperator(String msg,
      Function<ColumnIndexBase<?>.ValueComparator, PrimitiveIterator.OfInt> validatorOp,
      Function<ColumnIndexBase<?>.ValueComparator, PrimitiveIterator.OfInt> actualOp,
      ColumnIndexBase<?>.ValueComparator comparator) {
    ExecStats stats = new ExecStats();

    IntList expected = stats.measureLinear(validatorOp, comparator);
    IntList actual = stats.measureBinary(actualOp, comparator);

    Assert.assertEquals(msg, expected, actual);

    return stats;
  }

  @Test
  public void testEq() {
    for (int i = FROM - 1; i <= TO + 1; ++i) {
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::eq,
          BoundaryOrder.ASCENDING::eq,
          ASCENDING.createValueComparator(i));
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::eq,
          BoundaryOrder.DESCENDING::eq,
          DESCENDING.createValueComparator(i));
    }
    for (int i = SINGLE_FROM - 1; i <= SINGLE_TO + 1; ++i) {
      ColumnIndexBase<?>.ValueComparator singleComparator = SINGLE.createValueComparator(i);
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::eq,
          BoundaryOrder.ASCENDING::eq,
          singleComparator);
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::eq,
          BoundaryOrder.DESCENDING::eq,
          singleComparator);
    }
    ExecStats stats = new ExecStats();
    for (int i = RAND_FROM - 1; i <= RAND_TO + 1; ++i) {
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::eq,
          BoundaryOrder.ASCENDING::eq,
          RAND_ASCENDING.createValueComparator(i)));
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::eq,
          BoundaryOrder.DESCENDING::eq,
          RAND_DESCENDING.createValueComparator(i)));
    }
    LOGGER.info("Executed eq on random data (page count: {}, values searched: {}):\n{}", RAND_COUNT,
        RAND_TO - RAND_FROM + 2, stats);
  }

  @Test
  public void testGt() {
    for (int i = FROM - 1; i <= TO + 1; ++i) {
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::gt,
          BoundaryOrder.ASCENDING::gt,
          ASCENDING.createValueComparator(i));
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::gt,
          BoundaryOrder.DESCENDING::gt,
          DESCENDING.createValueComparator(i));
    }
    for (int i = SINGLE_FROM - 1; i <= SINGLE_TO + 1; ++i) {
      ColumnIndexBase<?>.ValueComparator singleComparator = SINGLE.createValueComparator(i);
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::gt,
          BoundaryOrder.ASCENDING::gt,
          singleComparator);
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::gt,
          BoundaryOrder.DESCENDING::gt,
          singleComparator);
    }
    ExecStats stats = new ExecStats();
    for (int i = RAND_FROM - 1; i <= RAND_TO + 1; ++i) {
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::gt,
          BoundaryOrder.ASCENDING::gt,
          RAND_ASCENDING.createValueComparator(i)));
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::gt,
          BoundaryOrder.DESCENDING::gt,
          RAND_DESCENDING.createValueComparator(i)));
    }
    LOGGER.info("Executed gt on random data (page count: {}, values searched: {}):\n{}", RAND_COUNT,
        RAND_TO - RAND_FROM + 2, stats);
  }

  @Test
  public void testGtEq() {
    for (int i = FROM - 1; i <= TO + 1; ++i) {
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::gtEq,
          BoundaryOrder.ASCENDING::gtEq,
          ASCENDING.createValueComparator(i));
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::gtEq,
          BoundaryOrder.DESCENDING::gtEq,
          DESCENDING.createValueComparator(i));
    }
    for (int i = SINGLE_FROM - 1; i <= SINGLE_TO + 1; ++i) {
      ColumnIndexBase<?>.ValueComparator singleComparator = SINGLE.createValueComparator(i);
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::gtEq,
          BoundaryOrder.ASCENDING::gtEq,
          singleComparator);
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::gtEq,
          BoundaryOrder.DESCENDING::gtEq,
          singleComparator);
    }
    ExecStats stats = new ExecStats();
    for (int i = RAND_FROM - 1; i <= RAND_TO + 1; ++i) {
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::gtEq,
          BoundaryOrder.ASCENDING::gtEq,
          RAND_ASCENDING.createValueComparator(i)));
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::gtEq,
          BoundaryOrder.DESCENDING::gtEq,
          RAND_DESCENDING.createValueComparator(i)));
    }
    LOGGER.info("Executed gtEq on random data (page count: {}, values searched: {}):\n{}", RAND_COUNT,
        RAND_TO - RAND_FROM + 2, stats);
  }

  @Test
  public void testLt() {
    for (int i = FROM - 1; i <= TO + 1; ++i) {
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::lt,
          BoundaryOrder.ASCENDING::lt,
          ASCENDING.createValueComparator(i));
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::lt,
          BoundaryOrder.DESCENDING::lt,
          DESCENDING.createValueComparator(i));
    }
    for (int i = SINGLE_FROM - 1; i <= SINGLE_TO + 1; ++i) {
      ColumnIndexBase<?>.ValueComparator singleComparator = SINGLE.createValueComparator(i);
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::lt,
          BoundaryOrder.ASCENDING::lt,
          singleComparator);
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::lt,
          BoundaryOrder.DESCENDING::lt,
          singleComparator);
    }
    ExecStats stats = new ExecStats();
    for (int i = RAND_FROM - 1; i <= RAND_TO + 1; ++i) {
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::lt,
          BoundaryOrder.ASCENDING::lt,
          RAND_ASCENDING.createValueComparator(i)));
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::lt,
          BoundaryOrder.DESCENDING::lt,
          RAND_DESCENDING.createValueComparator(i)));
    }
    LOGGER.info("Executed lt on random data (page count: {}, values searched: {}):\n{}", RAND_COUNT,
        RAND_TO - RAND_FROM + 2, stats);
  }

  @Test
  public void testLtEq() {
    for (int i = FROM - 1; i <= TO + 1; ++i) {
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::ltEq,
          BoundaryOrder.ASCENDING::ltEq,
          ASCENDING.createValueComparator(i));
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::ltEq,
          BoundaryOrder.DESCENDING::ltEq,
          DESCENDING.createValueComparator(i));
    }
    for (int i = SINGLE_FROM - 1; i <= SINGLE_TO + 1; ++i) {
      ColumnIndexBase<?>.ValueComparator singleComparator = SINGLE.createValueComparator(i);
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::ltEq,
          BoundaryOrder.ASCENDING::ltEq,
          singleComparator);
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::ltEq,
          BoundaryOrder.DESCENDING::ltEq,
          singleComparator);
    }
    ExecStats stats = new ExecStats();
    for (int i = RAND_FROM - 1; i <= RAND_TO + 1; ++i) {
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::ltEq,
          BoundaryOrder.ASCENDING::ltEq,
          RAND_ASCENDING.createValueComparator(i)));
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::ltEq,
          BoundaryOrder.DESCENDING::ltEq,
          RAND_DESCENDING.createValueComparator(i)));
    }
    LOGGER.info("Executed ltEq on random data (page count: {}, values searched: {}):\n{}", RAND_COUNT,
        RAND_TO - RAND_FROM + 2, stats);
  }

  @Test
  public void testNotEq() {
    for (int i = -16; i <= 16; ++i) {
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::notEq,
          BoundaryOrder.ASCENDING::notEq,
          ASCENDING.createValueComparator(i));
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::notEq,
          BoundaryOrder.DESCENDING::notEq,
          DESCENDING.createValueComparator(i));
    }
    for (int i = FROM - 1; i <= TO + 1; ++i) {
      ColumnIndexBase<?>.ValueComparator singleComparator = SINGLE.createValueComparator(i);
      validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::notEq,
          BoundaryOrder.ASCENDING::notEq,
          singleComparator);
      validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::notEq,
          BoundaryOrder.DESCENDING::notEq,
          singleComparator);
    }
    ExecStats stats = new ExecStats();
    for (int i = RAND_FROM - 1; i <= RAND_TO + 1; ++i) {
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with ASCENDING order",
          BoundaryOrder.UNORDERED::notEq,
          BoundaryOrder.ASCENDING::notEq,
          RAND_ASCENDING.createValueComparator(i)));
      stats.add(validateOperator("Mismatching page indexes for value " + i + " with DESCENDING order",
          BoundaryOrder.UNORDERED::notEq,
          BoundaryOrder.DESCENDING::notEq,
          RAND_DESCENDING.createValueComparator(i)));
    }
    LOGGER.info("Executed notEq on random data (page count: {}, values searched: {}):\n{}", RAND_COUNT,
        RAND_TO - RAND_FROM + 2, stats);
  }

}
