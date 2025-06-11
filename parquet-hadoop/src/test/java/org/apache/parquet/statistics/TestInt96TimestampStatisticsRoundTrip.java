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
package org.apache.parquet.statistics;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestInt96TimestampStatisticsRoundTrip {
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  private MessageType createSchema() {
    return Types.buildMessage()
        .required(INT96).named("timestamp_field")
        .named("root");
  }

  /**
   * Convert a timestamp string in format "yyyy-MM-ddTHH:mm:ss.SSS" to INT96 bytes using NanoTime.
   * INT96 timestamps in Parquet are encoded as 12 bytes where:
   * - First 8 bytes: nanoseconds from midnight
   * - Last 4 bytes: Julian day
   */
  private Binary timestampToInt96(String timestamp) {
    LocalDateTime dt = LocalDateTime.parse(timestamp);
    long julianDay = dt.toLocalDate().toEpochDay() + 2440588; // Convert to Julian Day
    long nanos = dt.toLocalTime().toNanoOfDay();
    return new NanoTime((int)julianDay, nanos).toBinary();
  }

  private void writeParquetFile(Path file, List<Binary> timestampValues) throws IOException {
    MessageType schema = createSchema();
    Configuration conf = new Configuration();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withConf(conf)
        .withType(schema)
        .build()) {
      for (Binary value : timestampValues) {
        Group group = new SimpleGroup(schema);
        group.add("timestamp_field", value);
        writer.write(group);
      }
    }
  }

  private void verifyStatistics(Path file, Binary minValue, Binary maxValue, boolean readInt96Stats) throws IOException {
    Configuration conf = new Configuration();
    conf.set("parquet.read.int96stats.enabled", readInt96Stats ? "true" : "false");
    ParquetMetadata metadata = ParquetFileReader.readFooter(conf, file);
    
    // Verify INT96 statistics
    ColumnChunkMetaData timestampColumn = metadata.getBlocks().get(0).getColumns().get(0);
    Statistics<?> timestampStats = timestampColumn.getStatistics();

    if (readInt96Stats) {
      assertTrue("INT96 statistics have non-null values", timestampStats.hasNonNullValue());
      assertEquals(Binary.fromConstantByteArray(timestampStats.getMinBytes()), minValue);
      assertEquals(Binary.fromConstantByteArray(timestampStats.getMaxBytes()), maxValue);
    } else {
      assertTrue("INT96 statistics should not be present", !timestampStats.hasNonNullValue());
      return;
    }
  }

  private void runTimestampTest(String[] timestamps) throws IOException {
    Binary minValue = timestampToInt96(timestamps[0]);
    Binary maxValue = timestampToInt96(timestamps[timestamps.length - 1]);
    List<Binary> timestampValues = new ArrayList<>();
    for (String timestamp : timestamps) {
        timestampValues.add(timestampToInt96(timestamp));
    }
    Collections.shuffle(timestampValues);

    Path file = new Path(temp.getRoot().getPath(), "test_timestamps.parquet");
    writeParquetFile(file, timestampValues);
    verifyStatistics(file, minValue, maxValue, false);
    verifyStatistics(file, minValue, maxValue, true);
  }

  @Test
  public void testMultipleDates() throws IOException {
    String[] timestamps = {
      "2020-01-01T00:00:00.000",
      "2020-02-29T23:59:59.000",
      "2020-12-31T23:59:59.000",
      "2021-01-01T00:00:00.000",
      "2023-06-15T12:30:45.000",
      "2024-02-29T15:45:30.000",
      "2024-12-25T07:00:00.000",
      "2025-01-01T00:00:00.000",
      "2025-07-04T20:00:00.000",
      "2025-12-31T23:59:59.000"
    };
    runTimestampTest(timestamps);
  }

  @Test
  public void testSameDayDifferentTime() throws IOException {
    String[] timestamps = {
      "2020-01-01T00:01:00.000",
      "2020-01-01T00:02:00.000",
      "2020-01-01T00:03:00.000"
    };
    runTimestampTest(timestamps);
  }

  @Test
  public void testIncreasingDayDecreasingTime() throws IOException {
    String[] timestamps = {
      "2020-01-01T12:00:00.000",
      "2020-02-01T11:00:00.000",
      "2020-03-01T10:00:00.000"
    };
    runTimestampTest(timestamps);
  }
}
