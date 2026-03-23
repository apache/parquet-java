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
package org.apache.parquet.column;

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Before;
import org.junit.Test;

public class TestParquetProperties {

  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
      "message test { required binary col_a; required int32 col_b; required double col_c; }");

  private ColumnDescriptor colA;
  private ColumnDescriptor colB;
  private ColumnDescriptor colC;

  @Before
  public void setUp() {
    colA = SCHEMA.getColumns().get(0);
    colB = SCHEMA.getColumns().get(1);
    colC = SCHEMA.getColumns().get(2);
  }

  @Test
  public void columnCodec_notSet_returnsNull() {
    ParquetProperties props = ParquetProperties.builder().build();
    assertNull(props.getColumnCodec(colA));
  }

  @Test
  public void columnLevel_notSet_returnsNull() {
    ParquetProperties props = ParquetProperties.builder().build();
    assertNull(props.getColumnCompressionLevel(colA));
  }

  @Test
  public void columnCodec_setForColumn_returnsConfiguredCodec() {
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionCodec("col_a", ZSTD)
        .build();
    assertEquals(ZSTD, props.getColumnCodec(colA));
  }

  @Test
  public void columnCodec_setMultipleTimes_lastValueWins() {
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionCodec("col_a", ZSTD)
        .withCompressionCodec("col_a", SNAPPY)
        .build();
    assertEquals(SNAPPY, props.getColumnCodec(colA));
  }

  @Test
  public void columnCodec_otherColumnsUnaffected() {
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionCodec("col_a", ZSTD)
        .build();
    assertNull(props.getColumnCodec(colB));
    assertNull(props.getColumnCodec(colC));
  }

  @Test
  public void columnLevel_setForColumn_returnsConfiguredLevel() {
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionLevel("col_a", 10)
        .build();
    assertEquals(Integer.valueOf(10), props.getColumnCompressionLevel(colA));
  }

  @Test
  public void columnLevel_otherColumnsUnaffected() {
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionLevel("col_a", 10)
        .build();
    assertNull(props.getColumnCompressionLevel(colB));
  }

  @Test
  public void columnCodecAndLevel_multipleColumns_eachGetsOwn() {
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionCodec("col_a", ZSTD)
        .withCompressionLevel("col_a", 10)
        .withCompressionCodec("col_b", SNAPPY)
        .withCompressionCodec("col_c", GZIP)
        .withCompressionLevel("col_c", 5)
        .build();

    assertEquals(ZSTD, props.getColumnCodec(colA));
    assertEquals(Integer.valueOf(10), props.getColumnCompressionLevel(colA));

    assertEquals(SNAPPY, props.getColumnCodec(colB));
    assertNull(props.getColumnCompressionLevel(colB));

    assertEquals(GZIP, props.getColumnCodec(colC));
    assertEquals(Integer.valueOf(5), props.getColumnCompressionLevel(colC));
  }

  @Test
  public void withCompressionCodec_nullCodec_throwsNullPointerException() {
    assertThrows(NullPointerException.class,
        () -> ParquetProperties.builder().withCompressionCodec("col_a", null));
  }

  @Test
  public void copyBuilder_preservesColumnCodecAndLevel() {
    ParquetProperties original = ParquetProperties.builder()
        .withCompressionCodec("col_a", ZSTD)
        .withCompressionLevel("col_a", 7)
        .withCompressionCodec("col_b", SNAPPY)
        .build();

    ParquetProperties copy = ParquetProperties.copy(original).build();

    assertEquals(ZSTD, copy.getColumnCodec(colA));
    assertEquals(Integer.valueOf(7), copy.getColumnCompressionLevel(colA));
    assertEquals(SNAPPY, copy.getColumnCodec(colB));
    assertNull(copy.getColumnCompressionLevel(colB));
    assertNull(copy.getColumnCodec(colC));
  }
}
