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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.DirectWriterTest;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestFileValueReadWrite extends DirectWriterTest {
  private static final GroupType FILE_SCHEMA = Types.optionalGroup()
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
  private static final MessageType SCHEMA = new MessageType("message", FILE_SCHEMA);

  @TempDir
  private java.nio.file.Path tempDir;

  @Test
  public void testFileValueRoundTrip() throws IOException {
    FileValue[] expectedValues = {
      FileValue.builder()
          .withUri("s3://bucket/file")
          .withOffset(10)
          .withSize(20)
          .withContentType("image/png")
          .withChecksum("ETAG:abc")
          .withInline(Binary.fromString("bytes"))
          .build(),
      FileValue.builder().withInline(Binary.fromString("inline only")).build(),
      null,
      FileValue.builder().withUri("relative/path").build(),
      FileValue.builder().withUri("file").withOffset(0).withSize(0).build(),
      FileValue.builder()
          .withInline(Binary.fromConstantByteArray(new byte[0]))
          .build()
    };
    Path path = new Path(tempDir.resolve("files.parquet").toUri());
    try (ParquetWriter<FileValue> writer = new ParquetWriter<>(path, new FileWriteSupport())) {
      for (FileValue value : expectedValues) {
        writer.write(value);
      }
    }

    try (ParquetReader<FileRecord> reader =
        ParquetReader.builder(new FileReadSupport(), path).build()) {
      for (FileValue expected : expectedValues) {
        FileRecord record = reader.read();
        assertThat(record).isNotNull();
        if (expected == null) {
          assertThat(record.value).isNull();
        } else {
          FileValue actual = record.value;
          assertThat(actual).isNotNull();
          assertThat(actual.getUri()).isEqualTo(expected.getUri());
          assertThat(actual.getOffset()).isEqualTo(expected.getOffset());
          assertThat(actual.getSize()).isEqualTo(expected.getSize());
          assertThat(actual.getContentType()).isEqualTo(expected.getContentType());
          assertThat(actual.getChecksum()).isEqualTo(expected.getChecksum());
          assertThat(actual.getInline()).isEqualTo(expected.getInline());
        }
      }
      assertThat(reader.read()).isNull();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInvalidStoredFileValueConvertsToNull(boolean sizeWithoutUri) throws IOException {
    Path path = writeDirect(SCHEMA, consumer -> {
      consumer.startMessage();
      consumer.startField("file", 0);
      consumer.startGroup();
      if (sizeWithoutUri) {
        consumer.startField("size", FILE_SCHEMA.getFieldIndex("size"));
        consumer.addLong(2);
        consumer.endField("size", FILE_SCHEMA.getFieldIndex("size"));
      }
      consumer.startField("content_type", FILE_SCHEMA.getFieldIndex("content_type"));
      consumer.addBinary(Binary.fromString("image/png"));
      consumer.endField("content_type", FILE_SCHEMA.getFieldIndex("content_type"));
      if (sizeWithoutUri) {
        consumer.startField("inline", FILE_SCHEMA.getFieldIndex("inline"));
        consumer.addBinary(Binary.fromString("bytes"));
        consumer.endField("inline", FILE_SCHEMA.getFieldIndex("inline"));
      }
      consumer.endGroup();
      consumer.endField("file", 0);
      consumer.endMessage();
    });

    try (ParquetReader<FileRecord> reader =
        ParquetReader.builder(new FileReadSupport(), path).build()) {
      assertThat(reader.read().value).isNull();
      assertThat(reader.read()).isNull();
    }
  }

  @Test
  public void testFileConverterRejectsPartialProjectionDuringReaderAssembly() throws IOException {
    Path path = writeDirect(SCHEMA, consumer -> {
      consumer.startMessage();
      consumer.startField("file", 0);
      FileValueWriter.write(
          consumer, FILE_SCHEMA, FileValue.builder().withUri("file").build());
      consumer.endField("file", 0);
      consumer.endMessage();
    });
    MessageType requestedSchema = new MessageType("message", FILE_SCHEMA.withNewFields(FILE_SCHEMA.getType("uri")));

    try (ParquetReader<FileRecord> reader = ParquetReader.builder(new FileReadSupport(requestedSchema), path)
        .build()) {
      assertThatThrownBy(reader::read)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("FILE group 'file' cannot project stored field 'offset'");
    }
  }

  @Test
  public void testFileConverterReadsReorderedSchemaWithAddedFields() throws IOException {
    GroupType storedFile = FILE_SCHEMA.withNewFields(FILE_SCHEMA.getType("uri"));
    Path path = writeDirect(new MessageType("message", storedFile), consumer -> {
      consumer.startMessage();
      consumer.startField("file", 0);
      FileValueWriter.write(
          consumer, storedFile, FileValue.builder().withUri("file").build());
      consumer.endField("file", 0);
      consumer.endMessage();
    });
    MessageType requestedSchema = new MessageType(
        "message", FILE_SCHEMA.withNewFields(FILE_SCHEMA.getType("inline"), FILE_SCHEMA.getType("uri")));

    try (ParquetReader<FileRecord> reader = ParquetReader.builder(new FileReadSupport(requestedSchema), path)
        .build()) {
      FileValue actual = reader.read().value;
      assertThat(actual.getUri()).isEqualTo("file");
      assertThat(actual.getInline()).isNull();
      assertThat(reader.read()).isNull();
    }
  }

  private static final class FileRecord {
    private final FileValue value;

    private FileRecord(FileValue value) {
      this.value = value;
    }
  }

  private static final class FileReadSupport extends ReadSupport<FileRecord> {
    private final MessageType requestedSchema;

    private FileReadSupport() {
      this(null);
    }

    private FileReadSupport(MessageType requestedSchema) {
      this.requestedSchema = requestedSchema;
    }

    @Override
    public ReadContext init(
        ParquetConfiguration configuration, Map<String, String> metadata, MessageType fileSchema) {
      return new ReadContext(requestedSchema == null ? fileSchema : requestedSchema);
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

  private static final class FileWriteSupport extends WriteSupport<FileValue> {
    private RecordConsumer consumer;

    @Override
    public WriteContext init(Configuration configuration) {
      return new WriteContext(SCHEMA, Collections.emptyMap());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      consumer = recordConsumer;
    }

    @Override
    public void write(FileValue value) {
      consumer.startMessage();
      if (value != null) {
        consumer.startField("file", 0);
        FileValueWriter.write(consumer, FILE_SCHEMA, value);
        consumer.endField("file", 0);
      }
      consumer.endMessage();
    }
  }

  private static final class FileRecordMaterializer extends RecordMaterializer<FileRecord> {
    private final GroupConverter root;
    private FileRecord current;

    private FileRecordMaterializer(MessageType fileSchema, MessageType requestedSchema) {
      GroupType requestedFile = requestedSchema.getType("file").asGroupType();
      GroupType storedFile = fileSchema.getType("file").asGroupType();
      GroupConverter fileConverter = FileConverters.newFileConverter(
          requestedFile, storedFile, value -> current = new FileRecord(value));
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
