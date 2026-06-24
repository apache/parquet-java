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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.junit.Test;

/**
 * Tests for {@link BlockMetaData#isApproach2()} sentinel detection. Approach 2 (micro-row-group)
 * blocks are detected by checking that every {@link ColumnChunkMetaData} in the block reports
 * {@link ColumnChunkMetaData#isPhysicallyShared()} as {@code true}.
 */
public class TestApproach2BlockMetaData {

  @Test
  public void testEmptyBlockIsNotApproach2() {
    BlockMetaData block = new BlockMetaData();
    assertFalse(block.isApproach2());
  }

  @Test
  public void testAllSentinelBlockIsApproach2() {
    BlockMetaData block = new BlockMetaData();
    block.addColumn(buildColumn("a", ColumnChunkMetaData.SENTINEL_OFFSET));
    block.addColumn(buildColumn("b", ColumnChunkMetaData.SENTINEL_OFFSET));
    assertTrue(block.isApproach2());
  }

  @Test
  public void testMixedSentinelBlockIsNotApproach2() {
    BlockMetaData block = new BlockMetaData();
    block.addColumn(buildColumn("a", ColumnChunkMetaData.SENTINEL_OFFSET));
    block.addColumn(buildColumn("b", 100L));
    assertFalse(block.isApproach2());
  }

  @Test
  public void testLegacyBlockIsNotApproach2() {
    BlockMetaData block = new BlockMetaData();
    block.addColumn(buildColumn("a", 100L));
    block.addColumn(buildColumn("b", 200L));
    assertFalse(block.isApproach2());
  }

  private static ColumnChunkMetaData buildColumn(String name, long firstDataPage) {
    return ColumnChunkMetaData.get(
        ColumnPath.get(name),
        BINARY,
        CompressionCodecName.UNCOMPRESSED,
        new HashSet<>(Collections.emptySet()),
        new BinaryStatistics(),
        firstDataPage,
        0L,
        0L,
        0L,
        0L);
  }
}
