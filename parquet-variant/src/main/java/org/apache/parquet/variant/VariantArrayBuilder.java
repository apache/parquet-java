/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.variant;

import java.util.ArrayList;

/**
 * Builder for creating Variant arrays, used by VariantBuilder.
 */
public class VariantArrayBuilder extends VariantBuilder {
  /** The offsets of the elements in this array. */
  private final ArrayList<Integer> offsets;
  /** The number of values appended to this array. */
  protected long numValues = 0;

  VariantArrayBuilder(Metadata metadata) {
    super(metadata);
    this.offsets = new ArrayList<>();
  }

  long numValues() {
    return numValues;
  }

  /**
   * @return the list of element offsets in this array
   */
  ArrayList<Integer> validateAndGetOffsets() {
    if (offsets.size() != numValues) {
      throw new IllegalStateException(String.format(
          "Number of offsets (%d) do not match the number of values (%d).", offsets.size(), numValues));
    }
    checkMultipleNested("Cannot call endArray() while a nested object/array is still open.");
    return offsets;
  }

  @Override
  protected void onAppend() {
    checkAppendWhileNested();
    offsets.add(writePos);
    numValues++;
  }

  @Override
  protected void onStartNested() {
    checkMultipleNested("Cannot call startObject()/startArray() without calling endObject()/endArray() first.");
    offsets.add(writePos);
    numValues++;
  }
}
