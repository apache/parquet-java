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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;

/** Writes and validates values represented by a Parquet {@code FILE}-annotated group. */
public final class FileValueWriter {
  private FileValueWriter() {}

  /**
   * Writes one FILE value. The caller is responsible for starting and ending the field containing
   * the FILE group.
   */
  public static void write(RecordConsumer recordConsumer, GroupType schema, FileValue value) {
    Objects.requireNonNull(recordConsumer, "recordConsumer cannot be null");
    Objects.requireNonNull(schema, "schema cannot be null");
    Objects.requireNonNull(value, "value cannot be null");

    FileValueValidator.validateSchema(schema);
    FileValueValidator.validateValue(value);
    validateSchemaContainsSetFields(schema, value);

    recordConsumer.startGroup();
    for (int index = 0; index < schema.getFieldCount(); index++) {
      String fieldName = schema.getFieldName(index);
      switch (fieldName) {
        case LogicalTypeAnnotation.FileLogicalTypeAnnotation.URI_FIELD:
          writeString(recordConsumer, fieldName, index, value.getUri());
          break;
        case LogicalTypeAnnotation.FileLogicalTypeAnnotation.OFFSET_FIELD:
          writeLong(recordConsumer, fieldName, index, value.getOffset());
          break;
        case LogicalTypeAnnotation.FileLogicalTypeAnnotation.SIZE_FIELD:
          writeLong(recordConsumer, fieldName, index, value.getSize());
          break;
        case LogicalTypeAnnotation.FileLogicalTypeAnnotation.CONTENT_TYPE_FIELD:
          writeString(recordConsumer, fieldName, index, value.getContentType());
          break;
        case LogicalTypeAnnotation.FileLogicalTypeAnnotation.CHECKSUM_FIELD:
          writeString(recordConsumer, fieldName, index, value.getChecksum());
          break;
        case LogicalTypeAnnotation.FileLogicalTypeAnnotation.INLINE_FIELD:
          writeBinary(recordConsumer, fieldName, index, value.getInline());
          break;
        default:
          throw new IllegalArgumentException(
              "Unrecognized field '" + fieldName + "' in FILE group '" + schema.getName() + "'");
      }
    }
    recordConsumer.endGroup();
  }

  private static void validateSchemaContainsSetFields(GroupType schema, FileValue value) {
    requireField(schema, LogicalTypeAnnotation.FileLogicalTypeAnnotation.URI_FIELD, value.getUri());
    requireField(schema, LogicalTypeAnnotation.FileLogicalTypeAnnotation.OFFSET_FIELD, value.getOffset());
    requireField(schema, LogicalTypeAnnotation.FileLogicalTypeAnnotation.SIZE_FIELD, value.getSize());
    requireField(schema, LogicalTypeAnnotation.FileLogicalTypeAnnotation.CONTENT_TYPE_FIELD, value.getContentType());
    requireField(schema, LogicalTypeAnnotation.FileLogicalTypeAnnotation.CHECKSUM_FIELD, value.getChecksum());
    requireField(schema, LogicalTypeAnnotation.FileLogicalTypeAnnotation.INLINE_FIELD, value.getInline());
  }

  private static void requireField(GroupType schema, String fieldName, Object value) {
    if (value != null && !schema.containsField(fieldName)) {
      throw new IllegalArgumentException(
          "FILE value sets field '" + fieldName + "' which is absent from group '" + schema.getName() + "'");
    }
  }

  private static void writeString(RecordConsumer consumer, String name, int index, String value) {
    if (value != null) {
      consumer.startField(name, index);
      consumer.addBinary(Binary.fromString(value));
      consumer.endField(name, index);
    }
  }

  private static void writeLong(RecordConsumer consumer, String name, int index, Long value) {
    if (value != null) {
      consumer.startField(name, index);
      consumer.addLong(value);
      consumer.endField(name, index);
    }
  }

  private static void writeBinary(RecordConsumer consumer, String name, int index, Binary value) {
    if (value != null) {
      consumer.startField(name, index);
      consumer.addBinary(value);
      consumer.endField(name, index);
    }
  }
}
