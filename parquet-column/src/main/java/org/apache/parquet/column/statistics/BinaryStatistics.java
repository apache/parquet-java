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
package org.apache.parquet.column.statistics;

import org.apache.parquet.filter2.predicate.ByteSignedness;
import org.apache.parquet.io.api.Binary;

/**
 * BinaryStatistics: tracks statistics information on binary data columns.
 * There are two sets of mins and maxes: those based on signed and unsigned byte comparisons.
 * For example, given a set of Binary values {@code Binary.fromString("a")}, {@code Binary.fromString("é")},
 * the signed min will be "é" as the first byte of the codepoint will be larger than 127
 */
public class BinaryStatistics extends Statistics<Binary> {

  private Binary maxSigned;
  private Binary minSigned;

  private Binary minUnsigned;
  private Binary maxUnsigned;

  @Override
  public void updateStats(Binary value) {
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    BinaryStatistics binaryStats = (BinaryStatistics)stats;
    if (!this.hasNonNullValue()) {
      initializeStats(binaryStats.genericGetMinSigned(), binaryStats.genericGetMaxSigned());
      initializeStats(binaryStats.genericGetMinUnsigned(), binaryStats.genericGetMaxUnsigned());
    } else {
      updateStats(binaryStats.genericGetMinSigned(), binaryStats.genericGetMaxSigned());
      updateStats(binaryStats.genericGetMinUnsigned(), binaryStats.genericGetMaxUnsigned());
    }
  }

  /**
   * Sets minSigned and maxSigned values, re-uses the byte[] passed in.
   * Any changes made to byte[] will be reflected in minSigned and maxSigned values as well.
   * @param minBytes byte array to set the minSigned value to
   * @param maxBytes byte array to set the maxSigned value to
   */
  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    maxSigned = Binary.fromReusedByteArray(maxBytes);
    minSigned = Binary.fromReusedByteArray(minBytes);
    maxUnsigned = maxSigned.copy();
    minUnsigned = minSigned.copy();
    this.markAsNotEmpty();
  }

  @Override
  public void setMinMaxSignedFromBytes(byte[] minBytes, byte[] maxBytes) {
    this.minSigned = Binary.fromReusedByteArray(minBytes);
    this.maxSigned = Binary.fromReusedByteArray(maxBytes);
  }

  @Override
  public void setMinMaxUnsignedFromBytes(byte[] minBytes, byte[] maxBytes) {
    this.minUnsigned = Binary.fromReusedByteArray(minBytes);
    this.maxUnsigned = Binary.fromReusedByteArray(maxBytes);
  }

  /**
   * Use either getMaxBytesSigned() or getMaxBytesUnsigned() directly instead.
   */
  @Deprecated
  @Override
  public byte[] getMaxBytes() {
    return getMaxBytesSigned();
  }

  /**
   * Use either getMinBytesSigned() or getMinBytesUnsigned() directly instead.
   */
  @Deprecated
  @Override
  public byte[] getMinBytes() {
    return getMinBytesSigned();
  }

  public byte[] getMaxBytesSigned() {
    return maxSigned == null ? null : maxSigned.getBytes();
  }

  @Override
  public byte[] getMinBytesSigned() {
    return minSigned == null ? null : minSigned.getBytes();
  }

  @Override
  public byte[] getMaxBytesUnsigned() {
    return maxUnsigned == null ? null : maxUnsigned.getBytes();
  }

  @Override
  public byte[] getMinBytesUnsigned() {
    return minUnsigned == null ? null : minUnsigned.getBytes();
  }

  @Override
  public boolean isSmallerThan(long size) {
    return !hasNonNullValue() || (((minSigned.length() + maxSigned.length()) < size) && ((minUnsigned.length() + maxUnsigned.length()) < size));
  }

  @Override
  public String toString() {
    if (this.hasNonNullValue())
      return String.format("min: %s, max: %s, num_nulls: %d", minSigned.toStringUsingUTF8(), maxSigned.toStringUsingUTF8(), this.getNumNulls());
   else if (!this.isEmpty())
      return String.format("num_nulls: %d, min/max not defined", this.getNumNulls());
   else
      return "no stats for this column";
  }

  /**
   * @deprecated use {@link #updateStats(Binary)}, will be removed in 2.0.0
   */
  @Deprecated
  public void updateStats(Binary min_value, Binary max_value) {
    if (minSigned.compareTo(min_value) > 0) { minSigned = min_value.copy(); }
    if (maxSigned.compareTo(max_value) < 0) { maxSigned = max_value.copy(); }
    if (Binary.compareTwoBinaryUnsigned(minUnsigned, min_value) > 0) { minUnsigned = min_value.copy(); }
    if (Binary.compareTwoBinaryUnsigned(maxUnsigned, max_value) < 0) { maxUnsigned = max_value.copy(); }
  }

  /**
   * @deprecated use {@link #updateStats(Binary)}, will be removed in 2.0.0
   */
  @Deprecated
  public void initializeStats(Binary min_value, Binary max_value) {
      minSigned = min_value.copy();
      maxSigned = max_value.copy();
      minUnsigned = min_value.copy();
      maxUnsigned = max_value.copy();
      this.markAsNotEmpty();
  }

  /**
   * For BinaryStatistics use one of genericGetMinSigned() or genericGetMinUnsigned()
   */
  @Deprecated
  @Override
  public Binary genericGetMin() {
    return genericGetMinSigned();
  }

  /**
   * For BinaryStatistics use one of genericGetMaxSigned() or generic getMaxUnsigned()
   */
  @Deprecated
  @Override
  public Binary genericGetMax() {
    return genericGetMaxSigned();
  }

  @Override
  public Binary genericGetMinSigned() {
    return minSigned;
  }

  @Override
  public Binary genericGetMaxSigned() {
    return maxSigned;
  }

  @Override
  public Binary genericGetMinUnsigned() {
    return minUnsigned;
  }

  @Override
  public Binary genericGetMaxUnsigned() {
    return maxUnsigned;
  }

  /**
   * @deprecated use {@link #genericGetMax()}, will be removed in 2.0.0
   */
  @Deprecated
  public Binary getMax() {
    return maxSigned;
  }

  /**
   * @deprecated use {@link #genericGetMin()}, will be removed in 2.0.0
   */
  @Deprecated
  public Binary getMin() {
    return minSigned;
  }

  /**
   * @deprecated use {@link #updateStats(Binary)}, will be removed in 2.0.0
   */
  @Deprecated
  public void setMinMax(Binary min, Binary max) {
    this.maxSigned = max;
    this.minSigned = min;
    this.maxUnsigned = max;
    this.minUnsigned = min;
    this.markAsNotEmpty();
  }

  @Override
  public final int compareValueToMin(Binary value, ByteSignedness signedness) {
    if (signedness == ByteSignedness.SIGNED) {
      return value.compareTo(genericGetMinSigned());
    } else {
      return Binary.compareTwoBinaryUnsigned(value, genericGetMinUnsigned());
    }
  }

  @Override
  public final int compareValueToMax(Binary value, ByteSignedness signedness) {
    if (signedness == ByteSignedness.SIGNED) {
      return value.compareTo(genericGetMaxSigned());
    } else {
      return Binary.compareTwoBinaryUnsigned(value, genericGetMaxUnsigned());
    }
  }

}
