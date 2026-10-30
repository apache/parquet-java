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

/**
 * Immutable per-column ALP encoding configuration.
 * <p>
 * Whether a column is ALP encoded at all is not part of this configuration: a column is ALP
 * encoded when it has an {@code AlpConfig} and is not when it has none.
 */
public final class AlpConfig {

  /** Default values per encoded vector. */
  public static final int DEFAULT_VECTOR_SIZE = AlpConstants.DEFAULT_VECTOR_SIZE;

  /** Configuration using the default vector size. */
  public static final AlpConfig DEFAULT = new AlpConfig(DEFAULT_VECTOR_SIZE);

  private final int vectorSize;

  /**
   * @param vectorSize values per encoded vector; must be a power of 2 in the supported range
   * @throws IllegalArgumentException if {@code vectorSize} is not a supported vector size
   */
  public AlpConfig(int vectorSize) {
    AlpConstants.validateVectorSize(vectorSize);
    this.vectorSize = vectorSize;
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
    return vectorSize == ((AlpConfig) o).vectorSize;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(vectorSize);
  }

  @Override
  public String toString() {
    return "AlpConfig{vectorSize=" + vectorSize + '}';
  }
}
