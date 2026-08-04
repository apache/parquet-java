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
package org.apache.parquet.filter2.bloomfilterlevel;

import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.in;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

public class TestBloomFilterImpl {
  private static final double DOUBLE_NAN = Double.longBitsToDouble(0x7ff8000000000001L);
  private static final float FLOAT_NAN = Float.intBitsToFloat(0x7fc00001);
  private static final Binary FLOAT16_NAN = Binary.fromConstantByteArray(new byte[] {0x01, 0x7e});

  @Test
  public void testDoubleNaNLiteralDoesNotDrop() {
    DoubleColumn column = doubleColumn("double_column");
    ColumnChunkMetaData meta = columnMeta("double_column", PrimitiveTypeName.DOUBLE);
    BloomFilterReader reader = bloomFilterReader(meta, new BlockSplitBloomFilter(0));

    assertThat(BloomFilterImpl.canDrop(eq(column, 1.0D), List.of(meta), reader))
        .isTrue();
    assertThat(BloomFilterImpl.canDrop(eq(column, DOUBLE_NAN), List.of(meta), reader))
        .isFalse();

    Set<Double> values = new HashSet<>();
    values.add(DOUBLE_NAN);
    assertThat(BloomFilterImpl.canDrop(in(column, values), List.of(meta), reader))
        .isFalse();
  }

  @Test
  public void testFloatNaNLiteralDoesNotDrop() {
    FloatColumn column = floatColumn("float_column");
    ColumnChunkMetaData meta = columnMeta("float_column", PrimitiveTypeName.FLOAT);
    BloomFilterReader reader = bloomFilterReader(meta, new BlockSplitBloomFilter(0));

    assertThat(BloomFilterImpl.canDrop(eq(column, 1.0F), List.of(meta), reader))
        .isTrue();
    assertThat(BloomFilterImpl.canDrop(eq(column, FLOAT_NAN), List.of(meta), reader))
        .isFalse();

    Set<Float> values = new HashSet<>();
    values.add(FLOAT_NAN);
    assertThat(BloomFilterImpl.canDrop(in(column, values), List.of(meta), reader))
        .isFalse();
  }

  @Test
  public void testFloat16NaNLiteralDoesNotDrop() {
    BinaryColumn column = binaryColumn("float16_column");
    PrimitiveType type = Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(2)
        .as(LogicalTypeAnnotation.float16Type())
        .named("float16_column");
    ColumnChunkMetaData meta = columnMeta("float16_column", type);
    BloomFilterReader reader = bloomFilterReader(meta, new BlockSplitBloomFilter(0));

    assertThat(BloomFilterImpl.canDrop(eq(column, FLOAT16_NAN), List.of(meta), reader))
        .isFalse();

    Set<Binary> values = new HashSet<>();
    values.add(FLOAT16_NAN);
    assertThat(BloomFilterImpl.canDrop(in(column, values), List.of(meta), reader))
        .isFalse();
  }

  private static ColumnChunkMetaData columnMeta(String name, PrimitiveTypeName type) {
    return columnMeta(name, Types.required(type).named(name));
  }

  private static ColumnChunkMetaData columnMeta(String name, PrimitiveType type) {
    Statistics<?> stats = Statistics.getBuilderForReading(type).build();
    return ColumnChunkMetaData.get(
        ColumnPath.get(name),
        type,
        CompressionCodecName.UNCOMPRESSED,
        null,
        new HashSet<>(List.of(Encoding.PLAIN)),
        stats,
        0L,
        0L,
        1L,
        0L,
        0L);
  }

  private static BloomFilterReader bloomFilterReader(ColumnChunkMetaData meta, BloomFilter bloomFilter) {
    BlockMetaData block = new BlockMetaData();
    block.addColumn(meta);
    return new BloomFilterReader(null, block) {
      @Override
      public BloomFilter readBloomFilter(ColumnChunkMetaData requestedMeta) {
        return requestedMeta == meta ? bloomFilter : null;
      }
    };
  }
}
