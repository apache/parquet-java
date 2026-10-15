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

import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

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
