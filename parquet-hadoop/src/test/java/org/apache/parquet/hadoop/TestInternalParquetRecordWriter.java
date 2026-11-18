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
package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.nio.file.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the scheduling of the row group size checks of {@link InternalParquetRecordWriter} (GH-2720).
 */
public class TestInternalParquetRecordWriter {

  private static final ParquetProperties PROPS = ParquetProperties.builder().build();
  private static final long MIN_CHECK = PROPS.getMinRowCountForPageSizeCheck();
  private static final long MAX_CHECK = PROPS.getMaxRowCountForPageSizeCheck();

  @TempDir
  private Path tempDir;

  @Test
  public void testNextMemCheckIsHalfwayToTheEstimatedRecordCount() {
    // 4000 records of 1000 bytes fit in the row group: check again halfway between 1000 and 4000
    assertThat(InternalParquetRecordWriter.nextMemCheck(1000, 1000, 4_000_000, PROPS))
        .isEqualTo(2500);
  }

  @Test
  public void testNextMemCheckIsAtLeastTheMinimum() {
    assertThat(InternalParquetRecordWriter.nextMemCheck(10, 1000, 20_000, PROPS))
        .isEqualTo(MIN_CHECK);
  }

  @Test
  public void testNextMemCheckIsAtMostTheMaximumAhead() {
    assertThat(InternalParquetRecordWriter.nextMemCheck(1000, 1, 128 * 1024 * 1024, PROPS))
        .isEqualTo(1000 + MAX_CHECK);
  }

  /**
   * Records smaller than a byte on average have an average size of 0 bytes. The estimated number of records in the
   * row group is then infinite, and used to overflow to a negative count, so that the size was checked after every
   * record.
   */
  @Test
  public void testNextMemCheckWithRecordsSmallerThanAByte() {
    assertThat(InternalParquetRecordWriter.nextMemCheck(1000, 0, 128 * 1024 * 1024, PROPS))
        .isEqualTo(1000 + MAX_CHECK);
    assertThat(InternalParquetRecordWriter.nextMemCheck(Integer.MAX_VALUE, 0, Long.MAX_VALUE, PROPS))
        .isEqualTo(Integer.MAX_VALUE + MAX_CHECK);
  }

  @Test
  public void testSizeIsNotCheckedForEveryRecordSmallerThanAByte() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required boolean flag; }");
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    Path file = tempDir.resolve("small-records.parquet");
    int recordCount = 100_000;
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
        .withType(schema)
        .build()) {
      for (int i = 0; i < recordCount; i++) {
        writer.write(factory.newGroup().append("flag", true));
      }
      InternalParquetRecordWriter<?> internalWriter = getField(writer, ParquetWriter.class, "writer");
      long written = getField(internalWriter, InternalParquetRecordWriter.class, "recordCount");
      long buffered = internalWriter.getDataSize();
      long nextMemCheck =
          getField(internalWriter, InternalParquetRecordWriter.class, "recordCountForNextMemCheck");

      // All the records are buffered in the first row group, in less than a byte each
      assertThat(written).isEqualTo(recordCount);
      assertThat(buffered).isPositive().isLessThan(recordCount);
      assertThat(nextMemCheck).isGreaterThan(written);
    }

    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
      assertThat(reader.getRecordCount()).isEqualTo(recordCount);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T getField(Object target, Class<?> type, String name) throws ReflectiveOperationException {
    Field field = type.getDeclaredField(name);
    field.setAccessible(true);
    return (T) field.get(target);
  }
}
