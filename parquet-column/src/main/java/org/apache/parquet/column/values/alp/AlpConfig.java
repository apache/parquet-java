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
package org.apache.parquet.column.values.alp;

import java.util.Objects;

/**
 * Immutable per-column ALP encoding configuration: whether ALP is enabled and the vector size
 * (number of values per encoded vector) to use. Bundled together so a column carries a single
 * cohesive ALP setting rather than several independent properties.
 */
public final class AlpConfig {

  /** Default values per encoded vector. */
  public static final int DEFAULT_VECTOR_SIZE = AlpConstants.DEFAULT_VECTOR_SIZE;

  /** ALP disabled, with the default vector size. */
  public static final AlpConfig DISABLED = new AlpConfig(false, DEFAULT_VECTOR_SIZE);

  private final boolean enabled;
  private final int vectorSize;

  /**
   * @param enabled    whether ALP encoding is enabled
   * @param vectorSize values per encoded vector; must be a power of 2 in the supported range
   * @throws IllegalArgumentException if {@code vectorSize} is not a supported vector size
   */
  public AlpConfig(boolean enabled, int vectorSize) {
    AlpConstants.validateVectorSize(vectorSize);
    this.enabled = enabled;
    this.vectorSize = vectorSize;
  }

  /** @return a copy of this config with {@code enabled} replaced. */
  public AlpConfig withEnabled(boolean enabled) {
    return new AlpConfig(enabled, vectorSize);
  }

  /** @return a copy of this config with {@code vectorSize} replaced. */
  public AlpConfig withVectorSize(int vectorSize) {
    return new AlpConfig(enabled, vectorSize);
  }

  public boolean isEnabled() {
    return enabled;
  }

  public int getVectorSize() {
    return vectorSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AlpConfig that = (AlpConfig) o;
    return enabled == that.enabled && vectorSize == that.vectorSize;
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled, vectorSize);
  }

  @Override
  public String toString() {
    return "AlpConfig{enabled=" + enabled + ", vectorSize=" + vectorSize + '}';
  }
}
