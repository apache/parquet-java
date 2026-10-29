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
package org.apache.parquet.file;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestFileConverters {
  private static final GroupType SCHEMA = Types.optionalGroup()
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
  public void testConvertsFileValue() {
    List<FileValue> values = new ArrayList<>();
    GroupConverter converter = FileConverters.newFileConverter(SCHEMA, SCHEMA, values::add);

    converter.start();
    converter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("s3://bucket/file"));
    converter.getConverter(1).asPrimitiveConverter().addLong(10);
    converter.getConverter(2).asPrimitiveConverter().addLong(20);
    converter.getConverter(3).asPrimitiveConverter().addBinary(Binary.fromString("image/png"));
    converter.getConverter(4).asPrimitiveConverter().addBinary(Binary.fromString("ETAG:abc"));
    converter.getConverter(5).asPrimitiveConverter().addBinary(Binary.fromString("bytes"));
    converter.end();

    assertThat(values).hasSize(1);
    FileValue value = values.get(0);
    assertThat(value.getUri()).isEqualTo("s3://bucket/file");
    assertThat(value.getOffset()).isEqualTo(10L);
    assertThat(value.getSize()).isEqualTo(20L);
    assertThat(value.getContentType()).isEqualTo("image/png");
    assertThat(value.getChecksum()).isEqualTo("ETAG:abc");
    assertThat(value.getInline().toStringUsingUTF8()).isEqualTo("bytes");
  }

  @Test
  public void testConvertsInvalidValueToNull() {
    List<FileValue> values = new ArrayList<>();
    GroupConverter converter = FileConverters.newFileConverter(SCHEMA, SCHEMA, values::add);

    converter.start();
    converter.getConverter(1).asPrimitiveConverter().addLong(10);
    converter.getConverter(2).asPrimitiveConverter().addLong(20);
    converter.end();

    assertThat(values).containsExactly((FileValue) null);
  }

  @Test
  public void testResetsBetweenValues() {
    List<FileValue> values = new ArrayList<>();
    GroupConverter converter = FileConverters.newFileConverter(SCHEMA, SCHEMA, values::add);

    converter.start();
    converter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("first"));
    converter.end();
    converter.start();
    converter.getConverter(5).asPrimitiveConverter().addBinary(Binary.fromString("second"));
    converter.end();

    assertThat(values).hasSize(2);
    assertThat(values.get(0).getUri()).isEqualTo("first");
    assertThat(values.get(0).getInline()).isNull();
    assertThat(values.get(1).getUri()).isNull();
    assertThat(values.get(1).getInline().toStringUsingUTF8()).isEqualTo("second");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSizeWithoutUriConvertsToNull(boolean emptyUri) {
    List<FileValue> values = new ArrayList<>();
    GroupConverter converter = FileConverters.newFileConverter(SCHEMA, SCHEMA, values::add);

    converter.start();
    if (emptyUri) {
      converter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString(""));
    }
    converter.getConverter(2).asPrimitiveConverter().addLong(2);
    converter.getConverter(5).asPrimitiveConverter().addBinary(Binary.fromString("bytes"));
    converter.end();

    assertThat(values).containsExactly((FileValue) null);
  }

  @Test
  public void testResetsAfterInvalidValue() {
    List<FileValue> values = new ArrayList<>();
    GroupConverter converter = FileConverters.newFileConverter(SCHEMA, SCHEMA, values::add);

    converter.start();
    converter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("first"));
    converter.getConverter(1).asPrimitiveConverter().addLong(1);
    converter.getConverter(2).asPrimitiveConverter().addLong(2);
    converter.getConverter(3).asPrimitiveConverter().addBinary(Binary.fromString("image/png"));
    converter.getConverter(4).asPrimitiveConverter().addBinary(Binary.fromString("ETAG:abc"));
    converter.end();
    converter.start();
    converter.getConverter(3).asPrimitiveConverter().addBinary(Binary.fromString("image/jpeg"));
    converter.end();
    converter.start();
    converter.getConverter(5).asPrimitiveConverter().addBinary(Binary.fromConstantByteArray(new byte[0]));
    converter.end();

    assertThat(values).hasSize(3);
    assertThat(values.get(0).getUri()).isEqualTo("first");
    assertThat(values.get(1)).isNull();
    FileValue last = values.get(2);
    assertThat(last.getInline().getBytes()).isEmpty();
    assertThat(last.getUri()).isNull();
    assertThat(last.getOffset()).isNull();
    assertThat(last.getSize()).isNull();
    assertThat(last.getContentType()).isNull();
    assertThat(last.getChecksum()).isNull();
  }

  @Test
  public void testCopiesReusedInlineBuffer() {
    List<FileValue> values = new ArrayList<>();
    GroupConverter converter = FileConverters.newFileConverter(SCHEMA, SCHEMA, values::add);
    byte[] bytes = {1, 2, 3};
    Binary binary = Binary.fromReusedByteArray(bytes);

    converter.start();
    converter.getConverter(5).asPrimitiveConverter().addBinary(binary);
    converter.end();
    bytes[0] = 4;
    converter.start();
    converter.getConverter(5).asPrimitiveConverter().addBinary(binary);
    converter.end();

    assertThat(values).hasSize(2);
    assertThat(values.get(0).getInline().getBytes()).containsExactly(1, 2, 3);
    assertThat(values.get(1).getInline().getBytes()).containsExactly(4, 2, 3);
  }

  @Test
  public void testRetainsConstantInlineBuffer() {
    List<FileValue> values = new ArrayList<>();
    GroupConverter converter = FileConverters.newFileConverter(SCHEMA, SCHEMA, values::add);
    Binary binary = Binary.fromConstantByteArray(new byte[] {1, 2, 3});

    converter.start();
    converter.getConverter(5).asPrimitiveConverter().addBinary(binary);
    converter.end();

    assertThat(values).hasSize(1);
    assertThat(values.get(0).getInline()).isSameAs(binary);
  }

  @Test
  public void testAllowsAddedFieldsAndReorderedProjection() {
    GroupType storedSchema = SCHEMA.withNewFields(SCHEMA.getType("uri"));
    GroupType requestedSchema = SCHEMA.withNewFields(SCHEMA.getType("inline"), SCHEMA.getType("uri"));
    List<FileValue> values = new ArrayList<>();
    GroupConverter converter = FileConverters.newFileConverter(requestedSchema, storedSchema, values::add);

    converter.start();
    converter.getConverter(1).asPrimitiveConverter().addBinary(Binary.fromString("s3://bucket/file"));
    converter.end();

    assertThat(values).hasSize(1);
    assertThat(values.get(0).getUri()).isEqualTo("s3://bucket/file");
    assertThat(values.get(0).getInline()).isNull();
  }

  @Test
  public void testRejectsPartialFileProjection() {
    GroupType projectedSchema = Types.optionalGroup()
        .as(LogicalTypeAnnotation.fileType())
        .optional(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("content_type")
        .named("file");

    assertThatThrownBy(() -> FileConverters.newFileConverter(projectedSchema, SCHEMA, value -> {}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("FILE group 'file' cannot project stored field 'uri'");
  }
}
