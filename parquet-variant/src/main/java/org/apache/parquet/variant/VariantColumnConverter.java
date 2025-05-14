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

import java.nio.ByteBuffer;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;

/**
 * Converter for shredded Variant values. Connectors should implement the addVariant method, similar to
 * the add* methods on PrimitiveConverter.
 */
public abstract class VariantColumnConverter extends VariantConverters.VariantElementConverter {

  private int topLevelMetadataIdx = -1;
  VariantBuilder builder = null;
  // We try to reuse metadata across rows, so track it outside of the builder.
  Metadata immutableMetadata = null;
  // Store the Binary metadata so that we can avoid reconstructing the metadata map if it's referentially equal
  // to the previous value. This should happen if all rows have the same metadata, and it's dictionary-encoded.
  Binary metadata = null;

  public VariantColumnConverter(GroupType variantSchema) {
    super(null, variantSchema);

    this.topLevelMetadataIdx = variantSchema.getFieldIndex("metadata");
    converters[topLevelMetadataIdx] = new VariantConverters.VariantMetadataConverter(this::setMetadata);
  }

  @Override
  public VariantBuilder getBuilder() {
    return this.builder;
  }

  /**
   * Set the final shredded value.
   */
  public abstract void addVariant(ByteBuffer value, ByteBuffer metadata);

  /**
   * Called at the beginning of the group managed by this converter.
   */
  @Override
  public void start() {
    super.start();
  }

  /**
   * Called at the end of the group.
   */
  @Override
  public void end() {
    super.end();
    ByteBuffer value = this.builder.valueWithoutMetadata();
    addVariant(value, builder.metadata.getEncodedBuffer());
  }

  private void setMetadata(Binary metadata) {
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
