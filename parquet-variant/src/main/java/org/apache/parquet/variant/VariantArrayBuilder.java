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
public class VariantArrayBuilder {
  VariantArrayBuilder(VariantBuilder builder) {
    this.builder = builder;
    this.startPos = builder.writePos();
    this.offsets = new ArrayList<>();
  }

  void startElement() {
    offsets.add(builder.writePos() - startPos);
  }

  int startPos() {
    return startPos;
  }

  ArrayList<Integer> offsets() {
    return offsets;
  }

  /** The underlying VariantBuilder. */
  private final VariantBuilder builder;
  /** The saved builder.writPos() for the start of this array. */
  private final int startPos;
  /** The offsets of the elements in this array. */
  private final ArrayList<Integer> offsets;
}
