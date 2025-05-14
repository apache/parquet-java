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
import org.apache.parquet.schema.GroupType;

/**
 * Converter for shredded Variant values. Connectors should implement the addVariant method, similar to
 * the add* methods on PrimitiveConverter.
 */
public abstract class VariantColumnConverter extends VariantConverters.VariantElementConverter {

  private int topLevelMetadataIdx = -1;

  public VariantColumnConverter(GroupType variantSchema) {
    super(variantSchema);

    this.topLevelMetadataIdx = variantSchema.getFieldIndex("metadata");
    converters[topLevelMetadataIdx] = new VariantConverters.VariantMetadataConverter();
    holder = new VariantBuilderTopLevelHolder();
    init(holder);
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
    ByteBuffer value = holder.builder.valueWithoutMetadata();
    addVariant(value, holder.getMetadata().toByteBuffer());
    // TODO: Don't do this. Right now, it's needed in order for getWritePos to work correctly, so we don't have
    // a stale builder in start().
    this.holder.builder = null;
  }
}
