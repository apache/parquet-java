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
package org.apache.parquet.arrow.writer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Computes Parquet page statistics from Arrow data buffers in a single sequential scan.
 * Thread-safe: all state is returned via {@link StatsResult}, no mutable shared fields.
 */
final class StatsComputer {

  private StatsComputer() {}

  /** Holds computed statistics plus NaN count for floating-point columns. */
  static final class StatsResult {
    final Statistics<?> statistics;
    final int nanCount;

    StatsResult(Statistics<?> statistics, int nanCount) {
      this.statistics = statistics;
      this.nanCount = nanCount;
    }
  }

  /**
   * Computes statistics for a non-null fixed-width column.
   *
   * @param dataBuf the Arrow data buffer
   * @param offset the starting row index
   * @param length the number of values
   * @param type the Parquet primitive type
   * @return statistics and NaN count (0 for non-floating-point types)
   */
  static StatsResult compute(ArrowBuf dataBuf, int offset, int length, PrimitiveType type) {
    switch (type.getPrimitiveTypeName()) {
      case INT32:
        return computeIntStats(dataBuf, offset, length, type);
      case INT64:
        return computeLongStats(dataBuf, offset, length, type);
      case FLOAT:
        return computeFloatStats(dataBuf, offset, length, type);
      case DOUBLE:
        return computeDoubleStats(dataBuf, offset, length, type);
      default:
        Statistics<?> stats = Statistics.createStats(type);
        stats.setNumNulls(0);
        return new StatsResult(stats, 0);
    }
  }

  private static StatsResult computeIntStats(
      ArrowBuf dataBuf, int offset, int length, PrimitiveType type) {
    ByteBuffer buf = dataBuf.nioBuffer(
        (long) offset * Integer.BYTES, (int) ((long) length * Integer.BYTES));
    buf.order(ByteOrder.LITTLE_ENDIAN);

    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    for (int i = 0; i < length; i++) {
      int v = buf.getInt();
      if (v < min) min = v;
      if (v > max) max = v;
    }

    IntStatistics stats = (IntStatistics) Statistics.createStats(type);
    stats.setMinMax(min, max);
    stats.setNumNulls(0);
    return new StatsResult(stats, 0);
  }

  private static StatsResult computeLongStats(
      ArrowBuf dataBuf, int offset, int length, PrimitiveType type) {
    ByteBuffer buf = dataBuf.nioBuffer(
        (long) offset * Long.BYTES, (int) ((long) length * Long.BYTES));
    buf.order(ByteOrder.LITTLE_ENDIAN);

    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int i = 0; i < length; i++) {
      long v = buf.getLong();
      if (v < min) min = v;
      if (v > max) max = v;
    }

    LongStatistics stats = (LongStatistics) Statistics.createStats(type);
    stats.setMinMax(min, max);
    stats.setNumNulls(0);
    return new StatsResult(stats, 0);
  }

  private static StatsResult computeFloatStats(
      ArrowBuf dataBuf, int offset, int length, PrimitiveType type) {
    ByteBuffer buf = dataBuf.nioBuffer(
        (long) offset * Float.BYTES, (int) ((long) length * Float.BYTES));
    buf.order(ByteOrder.LITTLE_ENDIAN);

    float min = Float.POSITIVE_INFINITY;
    float max = Float.NEGATIVE_INFINITY;
    int nanCount = 0;
    for (int i = 0; i < length; i++) {
      float v = buf.getFloat();
      if (Float.isNaN(v)) {
        nanCount++;
      } else {
        if (Float.compare(v, min) < 0) min = v;
        if (Float.compare(v, max) > 0) max = v;
      }
    }

    FloatStatistics stats = (FloatStatistics) Statistics.createStats(type);
    if (length - nanCount > 0) {
      stats.setMinMax(min, max);
    }
    stats.setNumNulls(0);
    return new StatsResult(stats, nanCount);
  }

  private static StatsResult computeDoubleStats(
      ArrowBuf dataBuf, int offset, int length, PrimitiveType type) {
    ByteBuffer buf = dataBuf.nioBuffer(
        (long) offset * Double.BYTES, (int) ((long) length * Double.BYTES));
    buf.order(ByteOrder.LITTLE_ENDIAN);

    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    int nanCount = 0;
    for (int i = 0; i < length; i++) {
      double v = buf.getDouble();
      if (Double.isNaN(v)) {
        nanCount++;
      } else {
        if (Double.compare(v, min) < 0) min = v;
        if (Double.compare(v, max) > 0) max = v;
      }
    }

    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(type);
    if (length - nanCount > 0) {
      stats.setMinMax(min, max);
    }
    stats.setNumNulls(0);
    return new StatsResult(stats, nanCount);
  }
}
