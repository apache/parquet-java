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
package org.apache.parquet.variant;

import org.apache.parquet.io.api.Binary;

/**
 * Interface for all converters in a shredded Variant schema. The `init` method is called by the top-level converter
 * after creating a tree of converters. Each converter is responsible for calling `init` on its child converters,
 * either with the same converter, or a new one in the case of object and array typed_values.
 */
interface VariantConverter {
  void init(VariantBuilderHolder builderHolder);

  /**
   * Stores the Variant builder and metadata used to rebuild a Variant value from its shredded representation.
   * Each object and array converter creates a new VariantBuilderHolder that constructs its builder from the parent
   * builder if the object or array is present in a given row.
   */
  static class VariantBuilderHolder {
    // The builder at this level. It may the the top-level builder, or an array or object builder.
    VariantBuilder builder = null;
    // The parent of this VariantBuilderHolder, or null if this is the top level.
    protected VariantBuilderHolder parentHolder;
    protected VariantBuilderTopLevelHolder topLevelHolder;

    VariantBuilderHolder() {}

    VariantBuilderHolder(VariantBuilderHolder parentHolder) {
      this.parentHolder = parentHolder;
      this.topLevelHolder = parentHolder.topLevelHolder;
    }

    // May only be called on an object converter.
    void startNewObject() {
      builder = parentHolder.builder.startObject();
    }

    // May only be called on an array converter.
    void startNewArray() {
      builder = parentHolder.builder.startArray();
    }

    Binary getMetadata() {
      return topLevelHolder.metadata;
    }
  }

  static class VariantBuilderTopLevelHolder extends VariantBuilderHolder {
    VariantBuilderTopLevelHolder() {
      super();
      this.topLevelHolder = this;
      this.parentHolder = null;
    }

    Binary metadata = null;
    Metadata immutableMetadata = null;

    /**
     * Sets the metadata. May only be called after startNewVariant. We allow the `value` column to
     * be added to the builder before metadata has been set, since it does not depend on metadata, but
     * typed_value (specifically, if typed_value is or contains an object) must be added after setting
     * the metadata.
     */
    void setMetadata(Binary metadata) {
      // If the metadata hasn't changed, we don't need to rebuild the map.
      // When metadata is dictionary encoded, we could consider keeping the map
      // around for every dictionary value, but that could be expensive, and handling adjacent
      // rows with identical metadata should be the most common case.
      if (this.metadata != metadata) {
        this.metadata = metadata;
        immutableMetadata = new ImmutableMetadata(metadata.toByteBuffer());
      }
      builder = new VariantBuilder(immutableMetadata);
    }
  }
}
