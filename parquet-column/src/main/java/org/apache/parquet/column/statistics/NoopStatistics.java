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

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

/**
 * A noop statistics which always return empty.
 */
class NoopStatistics<T extends Comparable<T>> extends Statistics<T> {

  NoopStatistics(PrimitiveType type) {
    super(type);
  }

  @Override
  public void updateStats(int value) {}

  @Override
  public void updateStats(long value) {}

  @Override
  public void updateStats(float value) {}

  @Override
  public void updateStats(double value) {}

  @Override
  public void updateStats(boolean value) {}

  @Override
  public void updateStats(Binary value) {}

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof Statistics)) return false;
    Statistics stats = (Statistics) other;
    return type().equals(stats.type());
  }

  @Override
  public int hashCode() {
    return 31 * type().hashCode();
  }

  @Override
  protected void mergeStatisticsMinMax(Statistics stats) {}

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {}

  @Override
  public T genericGetMin() {
    throw new UnsupportedOperationException(
        "genericGetMin is not supported by " + getClass().getName());
  }

  @Override
  public T genericGetMax() {
    throw new UnsupportedOperationException(
        "genericGetMax is not supported by " + getClass().getName());
  }

  @Override
  public byte[] getMaxBytes() {
    throw new UnsupportedOperationException(
        "getMaxBytes is not supported by " + getClass().getName());
  }

  @Override
  public byte[] getMinBytes() {
    throw new UnsupportedOperationException(
        "getMinBytes is not supported by " + getClass().getName());
  }

  @Override
  String stringify(T value) {
    throw new UnsupportedOperationException(
        "stringify is not supported by " + getClass().getName());
  }

  @Override
  public boolean isSmallerThan(long size) {
    throw new UnsupportedOperationException(
        "isSmallerThan is not supported by " + getClass().getName());
  }

  @Override
  public long getNumNulls() {
    return -1;
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  @Override
  public boolean hasNonNullValue() {
    return false;
  }

  @Override
  public boolean isNumNullsSet() {
    return false;
  }

  @Override
  public Statistics<T> copy() {
    return new NoopStatistics<>(this.type());
  }
}
