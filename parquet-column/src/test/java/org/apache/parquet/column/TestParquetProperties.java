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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestParquetProperties {

  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
      "message test { required binary col_a; required int32 col_b; required double col_c; }");

  private ColumnDescriptor colA;
  private ColumnDescriptor colB;
  private ColumnDescriptor colC;

  @BeforeEach
  public void setUp() {
    colA = SCHEMA.getColumns().get(0);
    colB = SCHEMA.getColumns().get(1);
    colC = SCHEMA.getColumns().get(2);
  }

  @Test
  public void columnCodec_notSet_returnsNull() {
    ParquetProperties props = ParquetProperties.builder().build();
    assertThat(props.getColumnCodec(colA)).isNull();
  }

  @Test
  public void columnLevel_notSet_returnsNull() {
    ParquetProperties props = ParquetProperties.builder().build();
    assertThat(props.getColumnCompressionLevel(colA)).isNull();
  }

  @Test
  public void columnCodec_setForColumn_returnsConfiguredCodec() {
    ParquetProperties props =
        ParquetProperties.builder().withCompressionCodec("col_a", ZSTD).build();
    assertThat(props.getColumnCodec(colA)).isEqualTo(ZSTD);
  }

  @Test
  public void columnCodec_setMultipleTimes_lastValueWins() {
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionCodec("col_a", ZSTD)
        .withCompressionCodec("col_a", SNAPPY)
        .build();
    assertThat(props.getColumnCodec(colA)).isEqualTo(SNAPPY);
  }

  @Test
  public void columnCodec_otherColumnsUnaffected() {
    ParquetProperties props =
        ParquetProperties.builder().withCompressionCodec("col_a", ZSTD).build();
    assertThat(props.getColumnCodec(colB)).isNull();
    assertThat(props.getColumnCodec(colC)).isNull();
  }

  @Test
  public void columnLevel_setForColumn_returnsConfiguredLevel() {
    ParquetProperties props =
        ParquetProperties.builder().withCompressionLevel("col_a", 10).build();
    assertThat(props.getColumnCompressionLevel(colA)).isEqualTo(10);
  }

  @Test
  public void columnLevel_otherColumnsUnaffected() {
    ParquetProperties props =
        ParquetProperties.builder().withCompressionLevel("col_a", 10).build();
    assertThat(props.getColumnCompressionLevel(colB)).isNull();
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

    assertThat(props.getColumnCodec(colA)).isEqualTo(ZSTD);
    assertThat(props.getColumnCompressionLevel(colA)).isEqualTo(10);

    assertThat(props.getColumnCodec(colB)).isEqualTo(SNAPPY);
    assertThat(props.getColumnCompressionLevel(colB)).isNull();

    assertThat(props.getColumnCodec(colC)).isEqualTo(GZIP);
    assertThat(props.getColumnCompressionLevel(colC)).isEqualTo(5);
  }

  @Test
  public void withCompressionCodec_nullCodec_throwsNullPointerException() {
    assertThatThrownBy(() -> ParquetProperties.builder().withCompressionCodec("col_a", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("codec cannot be null");
  }

  @Test
  public void pageCompressThreshold_rejectsValuesOutsideRatioRange() {
    assertThrows(IllegalArgumentException.class, () -> ParquetProperties.builder()
        .withPageCompressThreshold(-0.01));
    assertThrows(IllegalArgumentException.class, () -> ParquetProperties.builder()
        .withPageCompressThreshold(1.01));
    assertThrows(IllegalArgumentException.class, () -> ParquetProperties.builder()
        .withPageCompressThreshold(Double.NaN));
    assertThrows(IllegalArgumentException.class, () -> ParquetProperties.builder()
        .withPageCompressThreshold(Double.POSITIVE_INFINITY));
  }

  @Test
  public void pageCompressThreshold_acceptsRatioBoundaries() {
    assertEquals(
        0.0,
        ParquetProperties.builder()
            .withPageCompressThreshold(0.0)
            .build()
            .pageCompressThreshold(),
        0.0);
    assertEquals(
        1.0,
        ParquetProperties.builder()
            .withPageCompressThreshold(1.0)
            .build()
            .pageCompressThreshold(),
        0.0);
  }

  @Test
  public void copyBuilder_preservesColumnCodecAndLevel() {
    ParquetProperties original = ParquetProperties.builder()
        .withCompressionCodec("col_a", ZSTD)
        .withCompressionLevel("col_a", 7)
        .withCompressionCodec("col_b", SNAPPY)
        .build();

    ParquetProperties copy = ParquetProperties.copy(original).build();

    assertThat(copy.getColumnCodec(colA)).isEqualTo(ZSTD);
    assertThat(copy.getColumnCompressionLevel(colA)).isEqualTo(7);
    assertThat(copy.getColumnCodec(colB)).isEqualTo(SNAPPY);
    assertThat(copy.getColumnCompressionLevel(colB)).isNull();
    assertThat(copy.getColumnCodec(colC)).isNull();
  }
}
