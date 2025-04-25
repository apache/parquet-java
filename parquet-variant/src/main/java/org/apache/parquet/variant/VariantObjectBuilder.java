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
 * Builder for creating Variant object, used by VariantBuilder.
 */
public class VariantObjectBuilder {
  VariantObjectBuilder(VariantBuilder builder) {
    this.builder = builder;
    this.startPos = builder.writePos();
    this.fields = new ArrayList<>();
  }

  void appendKey(String key) {
    fields.add(new VariantBuilder.FieldEntry(key, builder.addKey(key), builder.writePos() - startPos));
  }

  int startPos() {
    return startPos;
  }

  ArrayList<VariantBuilder.FieldEntry> fields() {
    return fields;
  }

  /** The underlying VariantBuilder. */
  private final VariantBuilder builder;
  /** The saved builder.writPos() for the start of this object. */
  private final int startPos;
  /** The FieldEntry list for the fields of this object. */
  private final ArrayList<VariantBuilder.FieldEntry> fields;
}
