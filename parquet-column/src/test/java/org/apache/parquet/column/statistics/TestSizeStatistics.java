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
package org.apache.parquet.column.statistics;

import java.util.Arrays;
import java.util.Optional;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestSizeStatistics {

  @Test
  public void testAddBinaryType() {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("a");
    final int maxRepetitionLevel = 2;
    final int maxDefinitionLevel = 2;
    SizeStatistics.Builder builder = SizeStatistics.newBuilder(type, maxRepetitionLevel, maxDefinitionLevel);
    builder.add(0, 2, Binary.fromString("a"));
    builder.add(1, 2, Binary.fromString(""));
    builder.add(2, 2, Binary.fromString("bb"));
    builder.add(0, 0);
    builder.add(0, 1);
    builder.add(1, 0);
    builder.add(1, 1);
    SizeStatistics statistics = builder.build();
    Assert.assertEquals(Optional.of(3L), statistics.getUnencodedByteArrayDataBytes());
    Assert.assertEquals(Arrays.asList(3L, 3L, 1L), statistics.getRepetitionLevelHistogram());
    Assert.assertEquals(Arrays.asList(2L, 2L, 3L), statistics.getDefinitionLevelHistogram());
  }

  @Test
  public void testAddNonBinaryType() {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(2)
        .named("a");
    final int maxRepetitionLevel = 1;
    final int maxDefinitionLevel = 1;
    SizeStatistics.Builder builder = SizeStatistics.newBuilder(type, maxRepetitionLevel, maxDefinitionLevel);
    builder.add(0, 1, Binary.fromString("aa"));
    builder.add(0, 1, Binary.fromString("aa"));
    builder.add(1, 1, Binary.fromString("aa"));
    builder.add(1, 0);
    builder.add(1, 0);
    builder.add(1, 0);
    SizeStatistics statistics = builder.build();
    Assert.assertEquals(Optional.empty(), statistics.getUnencodedByteArrayDataBytes());
    Assert.assertEquals(Arrays.asList(2L, 4L), statistics.getRepetitionLevelHistogram());
    Assert.assertEquals(Arrays.asList(3L, 3L), statistics.getDefinitionLevelHistogram());
  }

  @Test
  public void testMergeStatistics() {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("a");
    final int maxRepetitionLevel = 2;
    final int maxDefinitionLevel = 2;
    SizeStatistics.Builder builder1 = SizeStatistics.newBuilder(type, maxRepetitionLevel, maxDefinitionLevel);
    builder1.add(0, 0, Binary.fromString("a"));
    builder1.add(1, 1, Binary.fromString("b"));
    builder1.add(2, 2, Binary.fromString("c"));
    SizeStatistics statistics1 = builder1.build();
    SizeStatistics.Builder builder2 = SizeStatistics.newBuilder(type, maxRepetitionLevel, maxDefinitionLevel);
    builder2.add(0, 1, Binary.fromString("d"));
    builder2.add(0, 1, Binary.fromString("e"));
    SizeStatistics statistics2 = builder2.build();
    statistics1.mergeStatistics(statistics2);
    Assert.assertEquals(Optional.of(5L), statistics1.getUnencodedByteArrayDataBytes());
    Assert.assertEquals(Arrays.asList(3L, 1L, 1L), statistics1.getRepetitionLevelHistogram());
    Assert.assertEquals(Arrays.asList(1L, 3L, 1L), statistics1.getDefinitionLevelHistogram());
  }

  @Test
  public void testMergeThrowException() {
    // Merge different types.
    PrimitiveType type1 = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("a");
    PrimitiveType type2 =
        Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("a");
    SizeStatistics.Builder builder1 = SizeStatistics.newBuilder(type1, 1, 1);
    SizeStatistics.Builder builder2 = SizeStatistics.newBuilder(type2, 1, 1);
    SizeStatistics statistics1 = builder1.build();
    SizeStatistics statistics2 = builder2.build();
    Assert.assertThrows(IllegalArgumentException.class, () -> statistics1.mergeStatistics(statistics2));
  }

  @Test
  public void testCopyStatistics() {
    PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("a");
    final int maxRepetitionLevel = 2;
    final int maxDefinitionLevel = 2;
    SizeStatistics.Builder builder = SizeStatistics.newBuilder(type, maxRepetitionLevel, maxDefinitionLevel);
    builder.add(0, 0, Binary.fromString("a"));
    builder.add(1, 1, Binary.fromString("b"));
    builder.add(2, 2, Binary.fromString("c"));
    SizeStatistics statistics = builder.build();
    SizeStatistics copy = statistics.copy();
    Assert.assertEquals(Optional.of(3L), copy.getUnencodedByteArrayDataBytes());
    Assert.assertEquals(Arrays.asList(1L, 1L, 1L), copy.getRepetitionLevelHistogram());
    Assert.assertEquals(Arrays.asList(1L, 1L, 1L), copy.getDefinitionLevelHistogram());
  }
}
