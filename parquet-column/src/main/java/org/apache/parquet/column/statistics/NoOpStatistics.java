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

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class NoOpStatistics extends Statistics<Boolean> {

  // A fake type object to be used to generate the proper comparator
  private static final PrimitiveType DEFAULT_FAKE_TYPE = Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
      .named("fake_boolean_type");
  private static final byte[] EMPTY_BYTES = new byte[0];

  private long max = 0;
  private long min = 0;

  /**
   * @deprecated will be removed in 2.0.0. Use {@link Statistics#createStats(org.apache.parquet.schema.Type)} instead
   */
  @Deprecated
  public NoOpStatistics() {
    this(DEFAULT_FAKE_TYPE);
  }

  NoOpStatistics(PrimitiveType type) {
    super(type);
  }

  private NoOpStatistics(NoOpStatistics other) {
    super(other.type());
  }

  @Override
  public void updateStats(long value) {
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
  }

  @Override
  public byte[] getMaxBytes() {
    return EMPTY_BYTES;
  }

  @Override
  public byte[] getMinBytes() {
    return EMPTY_BYTES;
  }

  @Override
  String stringify(Boolean value) {
    return "";
  }

  @Override
  public boolean isSmallerThan(long size) {
    return false;
  }

  @Override
  public Boolean genericGetMin() {
    return Boolean.FALSE;
  }

  @Override
  public Boolean genericGetMax() {
    return Boolean.FALSE;
  }

  public Boolean getMax() {
    return Boolean.FALSE;
  }

  public Boolean getMin() {
    return Boolean.FALSE;
  }

  @Override
  public NoOpStatistics copy() {
    return new NoOpStatistics(this);
  }
}
