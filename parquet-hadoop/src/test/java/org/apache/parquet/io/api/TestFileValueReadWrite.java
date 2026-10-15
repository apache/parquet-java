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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.DirectWriterTest;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

public class TestFileValueReadWrite extends DirectWriterTest {
  private static final GroupType FILE_SCHEMA = Types.optionalGroup()
      .as(LogicalTypeAnnotation.fileType())
      .optional(BINARY)
      .as(LogicalTypeAnnotation.stringType())
      .named("uri")
      .optional(BINARY)
      .as(LogicalTypeAnnotation.stringType())
      .named("content_type")
      .optional(BINARY)
      .named("inline")
      .named("file");
  private static final MessageType SCHEMA = new MessageType("message", FILE_SCHEMA);

  @Test
  public void testFileValueRoundTrip() throws IOException {
    FileValue expected = FileValue.builder()
        .withUri("s3://bucket/file")
        .withContentType("image/png")
        .build();
    Path path = writeDirect(SCHEMA, consumer -> {
      consumer.startMessage();
      consumer.startField("file", 0);
      FileValueWriter.write(consumer, FILE_SCHEMA, expected);
      consumer.endField("file", 0);
      consumer.endMessage();
    });

    try (ParquetReader<FileRecord> reader = ParquetReader.builder(new FileReadSupport(), path).build()) {
      FileValue actual = reader.read().value;
      assertThat(actual.getUri()).isEqualTo("s3://bucket/file");
      assertThat(actual.getContentType()).isEqualTo("image/png");
      assertThat(actual.getInline()).isNull();
    }
  }

  @Test
  public void testInvalidStoredFileValueConvertsToNull() throws IOException {
    Path path = writeDirect(SCHEMA, consumer -> {
      consumer.startMessage();
      consumer.startField("file", 0);
      consumer.startGroup();
      consumer.startField("content_type", 1);
      consumer.addBinary(Binary.fromString("image/png"));
      consumer.endField("content_type", 1);
      consumer.endGroup();
      consumer.endField("file", 0);
      consumer.endMessage();
    });

    try (ParquetReader<FileRecord> reader = ParquetReader.builder(new FileReadSupport(), path).build()) {
      assertThat(reader.read().value).isNull();
    }
  }

  private static final class FileRecord {
    private final FileValue value;

    private FileRecord(FileValue value) {
      this.value = value;
    }
  }

  private static final class FileReadSupport extends ReadSupport<FileRecord> {
    @Override
    public ReadContext init(ParquetConfiguration configuration, Map<String, String> metadata, MessageType fileSchema) {
      return new ReadContext(fileSchema);
    }

    @Override
    public RecordMaterializer<FileRecord> prepareForRead(
        Configuration configuration,
        Map<String, String> metadata,
        MessageType fileSchema,
        ReadContext readContext) {
      return new FileRecordMaterializer(fileSchema, readContext.getRequestedSchema());
    }
  }

  private static final class FileRecordMaterializer extends RecordMaterializer<FileRecord> {
    private final GroupConverter root;
    private FileRecord current;

    private FileRecordMaterializer(MessageType fileSchema, MessageType requestedSchema) {
      GroupType requestedFile = requestedSchema.getType("file").asGroupType();
      GroupType storedFile = fileSchema.getType("file").asGroupType();
      GroupConverter fileConverter =
          FileConverters.newFileConverter(requestedFile, storedFile, value -> current = new FileRecord(value));
      root = new GroupConverter() {
        @Override
        public Converter getConverter(int fieldIndex) {
          return fileConverter;
        }

        @Override
        public void start() {
          current = new FileRecord(null);
        }

        @Override
        public void end() {}
      };
    }

    @Override
    public FileRecord getCurrentRecord() {
      return current;
    }

    @Override
    public GroupConverter getRootConverter() {
      return root;
    }
  }
}
