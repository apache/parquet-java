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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import java.util.List;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

/**
 * Converter for shredded Variant values. Connectors should implement the addVariant method, similar to
 * the add* methods on PrimitiveConverter.
 */
public abstract class VariantColumnConverter extends VariantElementConverter {

  private int topLevelMetadataIdx = -1;

  public VariantColumnConverter(GroupType variantSchema) {
    super(variantSchema);

    List<Type> fields = variantSchema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      Type field = fields.get(i);
      String fieldName = field.getName();
      if (fieldName.equals("metadata")) {
        this.topLevelMetadataIdx = i;
        if (!field.isPrimitive() || field.asPrimitiveType().getPrimitiveTypeName() != BINARY) {
          throw new IllegalArgumentException("Metadata must be a binary value");
        }
      }
    }
    if (topLevelMetadataIdx < 0) {
      throw new IllegalArgumentException("Metadata missing from schema");
    }
    converters[topLevelMetadataIdx] = new VariantMetadataConverter();
    holder = new VariantBuilderTopLevelHolder();
    init(holder);
  }

  /**
   * Set the final shredded value.
   */
  public abstract void addVariant(Binary value, Binary metadata);

  /**
   * Called at the beginning of the group managed by this converter.
   */
  @Override
  public void start() {
    holder.startNewVariant();
    super.start();
  }

  /**
   * Called at the end of the group.
   */
  @Override
  public void end() {
    super.end();
    byte[] value = holder.builder.valueWithoutMetadata();
    addVariant(Binary.fromConstantByteArray(value), holder.getMetadata());
  }
}
