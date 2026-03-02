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

import static org.junit.Assert.assertEquals;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.Test;

public class ParquetPropertiesCompressionTest {

  private static ColumnDescriptor col(String name) {
    return new ColumnDescriptor(new String[] {name}, PrimitiveTypeName.BINARY, 0, 0);
  }

  @Test
  public void testDefaultCompressionCodecIsUncompressed() {
    ParquetProperties props = ParquetProperties.builder().build();
    assertEquals(CompressionCodecName.UNCOMPRESSED, props.getCompressionCodec(col("any_column")));
    assertEquals(CompressionCodecName.UNCOMPRESSED, props.getDefaultCompressionCodec());
  }

  @Test
public void testSetDefaultCompressionCodec() {
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();
    assertEquals(CompressionCodecName.SNAPPY, props.getCompressionCodec(col("any_column")));
    assertEquals(CompressionCodecName.SNAPPY, props.getDefaultCompressionCodec());
  }

  @Test
  public void testPerColumnCompressionCodec() {
    ParquetProperties props = ParquetProperties.builder()
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withCompressionCodec("col_a", CompressionCodecName.GZIP)
        .withCompressionCodec("col_b", CompressionCodecName.UNCOMPRESSED)
        .build();

    // Per-column overrides
    assertEquals(CompressionCodecName.GZIP, props.getCompressionCodec(col("col_a")));
    assertEquals(CompressionCodecName.UNCOMPRESSED, props.getCompressionCodec(col("col_b")));
    // Default for non-overridden columns
    assertEquals(CompressionCodecName.SNAPPY, props.getCompressionCodec(col("col_c")));
    assertEquals(CompressionCodecName.SNAPPY, props.getDefaultCompressionCodec());
  }

  @Test
  public void testCopyPreservesCompressionCodec() {
    ParquetProperties original = ParquetProperties.builder()
        .withCompressionCodec(CompressionCodecName.GZIP)
        .withCompressionCodec("col_a", CompressionCodecName.SNAPPY)
        .build();

    ParquetProperties copy = ParquetProperties.copy(original).build();

    assertEquals(CompressionCodecName.GZIP, copy.getCompressionCodec(col("other")));
    assertEquals(CompressionCodecName.SNAPPY, copy.getCompressionCodec(col("col_a")));
  }

  @Test
  public void testCopyCanOverrideDefault() {
    ParquetProperties original = ParquetProperties.builder()
        .withCompressionCodec(CompressionCodecName.GZIP)
        .withCompressionCodec("col_a", CompressionCodecName.SNAPPY)
        .build();

    ParquetProperties modified = ParquetProperties.copy(original)
        .withCompressionCodec(CompressionCodecName.ZSTD)
        .build();

    // Default overridden
    assertEquals(CompressionCodecName.ZSTD, modified.getCompressionCodec(col("other")));
    // Per-column override preserved
    assertEquals(CompressionCodecName.SNAPPY, modified.getCompressionCodec(col("col_a")));
  }

  @Test(expected = NullPointerException.class)
  public void testNullDefaultCompressionCodecThrows() {
    ParquetProperties.builder().withCompressionCodec((CompressionCodecName) null);
  }

  @Test(expected = NullPointerException.class)
  public void testNullPerColumnCompressionCodecThrows() {
    ParquetProperties.builder().withCompressionCodec("col_a", null);
  }
}
