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
package org.apache.parquet.hadoop.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import org.apache.parquet.VersionParser.VersionParseException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

class FileMetaDataTest {

  private static final MessageType SCHEMA = new MessageType(
      "test", new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "id"));

  @Test
  void validCreatedByIsParsed() throws Exception {
    FileMetaData meta =
        new FileMetaData(SCHEMA, Collections.emptyMap(), "parquet-mr version 1.12.0 (build abc123)");

    assertThat(meta.getWriterVersion()).isNotNull();
    assertThat(meta.getWriterVersion().application).isEqualTo("parquet-mr");
    assertThat(meta.getWriterVersion().version).isEqualTo("1.12.0");
    assertThat(meta.getWriterVersion().appBuildHash).isEqualTo("abc123");
  }

  @Test
  void nullCreatedByReturnsNullWriterVersion() throws Exception {
    FileMetaData meta = new FileMetaData(SCHEMA, Collections.emptyMap(), null);

    assertThat(meta.getWriterVersion()).isNull();
    assertThat(meta.getCreatedBy()).isNull();
  }

  @Test
  void emptyCreatedByReturnsNullWriterVersion() throws Exception {
    FileMetaData meta = new FileMetaData(SCHEMA, Collections.emptyMap(), "");

    assertThat(meta.getWriterVersion()).isNull();
  }

  @Test
  void unparseableCreatedByThrowsVersionParseException() {
    FileMetaData meta = new FileMetaData(SCHEMA, Collections.emptyMap(), "no-version-here");

    assertThatThrownBy(meta::getWriterVersion).isInstanceOf(VersionParseException.class);
  }

  @Test
  void versionWithoutBuildHash() throws Exception {
    FileMetaData meta = new FileMetaData(SCHEMA, Collections.emptyMap(), "parquet-mr version 1.8.0");

    assertThat(meta.getWriterVersion()).isNotNull();
    assertThat(meta.getWriterVersion().application).isEqualTo("parquet-mr");
    assertThat(meta.getWriterVersion().version).isEqualTo("1.8.0");
    assertThat(meta.getWriterVersion().appBuildHash).isNull();
  }

  @Test
  void writerVersionIsCached() throws Exception {
    FileMetaData meta = new FileMetaData(SCHEMA, Collections.emptyMap(), "parquet-mr version 1.12.0 (build abc)");

    assertThat(meta.getWriterVersion()).isSameAs(meta.getWriterVersion());
  }
}
