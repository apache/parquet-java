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
package org.apache.parquet.io.api;

import java.util.Objects;
import java.util.function.Consumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

/** Converters for assembling values represented by a Parquet {@code FILE}-annotated group. */
public final class FileConverters {
  private FileConverters() {}

  /**
   * Creates a converter for a possibly projected FILE group.
   *
   * <p>FILE values are atomic and cannot be projected by child field. The projected schema may
   * contain fields absent from this particular file for schema-evolution purposes, but it must
   * contain every FILE field stored in the file schema. The consumer receives {@code null} when an
   * invalid FILE reference is encountered, as permitted by the FILE logical-type specification.
   *
   * @param schema schema presented to the converter
   * @param fileSchema FILE group stored in the file
   * @param consumer consumer for converted values
   * @return a converter for the complete FILE value
   * @throws IllegalArgumentException if the projected schema omits a stored FILE field
   */
  public static GroupConverter newFileConverter(
      GroupType schema, GroupType fileSchema, Consumer<FileValue> consumer) {
    Objects.requireNonNull(schema, "schema cannot be null");
    Objects.requireNonNull(fileSchema, "fileSchema cannot be null");
    Objects.requireNonNull(consumer, "consumer cannot be null");
    FileValueValidator.validateSchema(schema);
    FileValueValidator.validateSchema(fileSchema);
    for (Type field : fileSchema.getFields()) {
      if (!schema.containsField(field.getName())) {
        throw new IllegalArgumentException(
            "FILE group '" + schema.getName() + "' cannot project stored field '" + field.getName() + "'");
      }
    }
    return new FileGroupConverter(schema, consumer);
  }

  private static final class FileGroupConverter extends GroupConverter {
    private final Converter[] converters;
    private final Consumer<FileValue> consumer;
    private FileValue.Builder builder;

    private FileGroupConverter(GroupType schema, Consumer<FileValue> consumer) {
      this.converters = new Converter[schema.getFieldCount()];
      this.consumer = consumer;
      for (int index = 0; index < schema.getFieldCount(); index++) {
        String fieldName = schema.getFieldName(index);
        switch (fieldName) {
          case LogicalTypeAnnotation.FileLogicalTypeAnnotation.URI_FIELD:
            converters[index] = stringConverter(value -> builder.withUri(value));
            break;
          case LogicalTypeAnnotation.FileLogicalTypeAnnotation.OFFSET_FIELD:
            converters[index] = longConverter(value -> builder.withOffset(value));
            break;
          case LogicalTypeAnnotation.FileLogicalTypeAnnotation.SIZE_FIELD:
            converters[index] = longConverter(value -> builder.withSize(value));
            break;
          case LogicalTypeAnnotation.FileLogicalTypeAnnotation.CONTENT_TYPE_FIELD:
            converters[index] = stringConverter(value -> builder.withContentType(value));
            break;
          case LogicalTypeAnnotation.FileLogicalTypeAnnotation.CHECKSUM_FIELD:
            converters[index] = stringConverter(value -> builder.withChecksum(value));
            break;
          case LogicalTypeAnnotation.FileLogicalTypeAnnotation.INLINE_FIELD:
            converters[index] = binaryConverter(value -> builder.withInline(value));
            break;
          default:
            throw new IllegalArgumentException("Unrecognized FILE field: " + fieldName);
        }
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
      builder = FileValue.builder();
    }

    @Override
    public void end() {
      FileValue value = builder.build();
      boolean valid = true;
      try {
        FileValueValidator.validateValue(value);
      } catch (IllegalArgumentException e) {
        valid = false;
      }
      builder = null;
      consumer.accept(valid ? value : null);
    }
  }

  private static PrimitiveConverter stringConverter(Consumer<String> consumer) {
    return new PrimitiveConverter() {
      @Override
      public void addBinary(Binary value) {
        consumer.accept(value.toStringUsingUTF8());
      }
    };
  }

  private static PrimitiveConverter binaryConverter(Consumer<Binary> consumer) {
    return new PrimitiveConverter() {
      @Override
      public void addBinary(Binary value) {
        consumer.accept(Binary.fromConstantByteArray(value.getBytes()));
      }
    };
  }

  private static PrimitiveConverter longConverter(Consumer<Long> consumer) {
    return new PrimitiveConverter() {
      @Override
      public void addLong(long value) {
        consumer.accept(value);
      }
    };
  }
}
