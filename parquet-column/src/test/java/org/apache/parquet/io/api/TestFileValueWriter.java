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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import org.apache.parquet.io.ExpectationValidatingRecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

public class TestFileValueWriter {
  private static final GroupType ALL_FIELDS_SCHEMA = Types.optionalGroup()
      .as(LogicalTypeAnnotation.fileType())
      .optional(BINARY)
      .as(LogicalTypeAnnotation.stringType())
      .named("uri")
      .optional(INT64)
      .named("offset")
      .optional(INT64)
      .named("size")
      .optional(BINARY)
      .as(LogicalTypeAnnotation.stringType())
      .named("content_type")
      .optional(BINARY)
      .as(LogicalTypeAnnotation.stringType())
      .named("checksum")
      .optional(BINARY)
      .named("inline")
      .named("file");

  @Test
  public void testWritesSetFieldsInSchemaOrder() {
    FileValue value = FileValue.builder()
        .withUri("s3://bucket/file")
        .withOffset(10)
        .withSize(20)
        .withContentType("image/png")
        .withChecksum("ETAG:abc")
        .withInline(Binary.fromString("bytes"))
        .build();
    Deque<String> expectations = new ArrayDeque<>(Arrays.asList(
        "startGroup()",
        "startField(uri, 0)",
        "addBinary(s3://bucket/file)",
        "endField(uri, 0)",
        "startField(offset, 1)",
        "addLong(10)",
        "endField(offset, 1)",
        "startField(size, 2)",
        "addLong(20)",
        "endField(size, 2)",
        "startField(content_type, 3)",
        "addBinary(image/png)",
        "endField(content_type, 3)",
        "startField(checksum, 4)",
        "addBinary(ETAG:abc)",
        "endField(checksum, 4)",
        "startField(inline, 5)",
        "addBinary(bytes)",
        "endField(inline, 5)",
        "endGroup()"));

    FileValueWriter.write(new ExpectationValidatingRecordConsumer(expectations), ALL_FIELDS_SCHEMA, value);

    assertThat(expectations).isEmpty();
  }

  @Test
  public void testWritesInlineOnlyValue() {
    GroupType schema = Types.optionalGroup()
        .as(LogicalTypeAnnotation.fileType())
        .optional(BINARY)
        .named("inline")
        .named("file");
    FileValue value = FileValue.builder()
        .withInline(Binary.fromConstantByteArray(new byte[0]))
        .build();
    Deque<String> expectations = new ArrayDeque<>(Arrays.asList(
        "startGroup()", "startField(inline, 0)", "addBinary()", "endField(inline, 0)", "endGroup()"));

    FileValueWriter.write(new ExpectationValidatingRecordConsumer(expectations), schema, value);

    assertThat(expectations).isEmpty();
  }

  @Test
  public void testRejectsUnresolvableValue() {
    FileValue value = FileValue.builder().withContentType("image/png").build();

    assertThatThrownBy(() -> FileValueWriter.write(noopConsumer(), ALL_FIELDS_SCHEMA, value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("FILE value must set at least one of 'inline' or non-empty 'uri'");
  }

  @Test
  public void testRejectsOffsetWithoutUri() {
    FileValue value = FileValue.builder()
        .withOffset(1)
        .withSize(2)
        .withInline(Binary.fromString("bytes"))
        .build();

    assertThatThrownBy(() -> FileValueWriter.write(noopConsumer(), ALL_FIELDS_SCHEMA, value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("FILE value field 'offset' may only be set together with 'uri'");
  }

  @Test
  public void testRejectsOffsetWithoutSize() {
    FileValue value = FileValue.builder().withUri("file").withOffset(1).build();

    assertThatThrownBy(() -> FileValueWriter.write(noopConsumer(), ALL_FIELDS_SCHEMA, value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("FILE value field 'size' must be set whenever 'offset' is set");
  }

  @Test
  public void testRejectsNegativeOffsetAndSize() {
    FileValue negativeOffset =
        FileValue.builder().withUri("file").withOffset(-1).withSize(1).build();
    FileValue negativeSize =
        FileValue.builder().withUri("file").withSize(-1).build();

    assertThatThrownBy(() -> FileValueWriter.write(noopConsumer(), ALL_FIELDS_SCHEMA, negativeOffset))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("FILE value field 'offset' must not be negative: -1");
    assertThatThrownBy(() -> FileValueWriter.write(noopConsumer(), ALL_FIELDS_SCHEMA, negativeSize))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("FILE value field 'size' must not be negative: -1");
  }

  @Test
  public void testRejectsSetFieldMissingFromSchema() {
    GroupType schema = Types.optionalGroup()
        .as(LogicalTypeAnnotation.fileType())
        .optional(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("uri")
        .named("file");
    FileValue value = FileValue.builder().withUri("file").withSize(10).build();

    assertThatThrownBy(() -> FileValueWriter.write(noopConsumer(), schema, value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("FILE value sets field 'size' which is absent from group 'file'");
  }

  private static RecordConsumer noopConsumer() {
    return new RecordConsumer() {
      @Override
      public void startMessage() {}

      @Override
      public void endMessage() {}

      @Override
      public void startField(String field, int index) {}

      @Override
      public void endField(String field, int index) {}

      @Override
      public void startGroup() {}

      @Override
      public void endGroup() {}

      @Override
      public void addInteger(int value) {}

      @Override
      public void addLong(long value) {}

      @Override
      public void addBoolean(boolean value) {}

      @Override
      public void addBinary(Binary value) {}

      @Override
      public void addFloat(float value) {}

      @Override
      public void addDouble(double value) {}
    };
  }
}
