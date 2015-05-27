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
package org.apache.parquet.filter2.compat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;

import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import static org.junit.Assert.assertEquals;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.hadoop.TestInputFormat.makeBlockFromStats;

public class TestRowGroupFilter {
  private List<BlockMetaData> blocks;
  private BlockMetaData b1;
  private BlockMetaData b2;
  private BlockMetaData b3;
  private BlockMetaData b4;
  private BlockMetaData b5;
  private BlockMetaData b6;
  private MessageType schema;
  private IntColumn foo;

  @Before
  public void setUp() throws Exception {
    blocks = new ArrayList<BlockMetaData>();

    IntStatistics stats1 = new IntStatistics();
    stats1.setMinMax(10, 100);
    stats1.setNumNulls(4);
    b1 = makeBlockFromStats(stats1, 301);
    blocks.add(b1);

    IntStatistics stats2 = new IntStatistics();
    stats2.setMinMax(8, 102);
    stats2.setNumNulls(0);
    b2 = makeBlockFromStats(stats2, 302);
    blocks.add(b2);

    IntStatistics stats3 = new IntStatistics();
    stats3.setMinMax(100, 102);
    stats3.setNumNulls(12);
    b3 = makeBlockFromStats(stats3, 303);
    blocks.add(b3);


    IntStatistics stats4 = new IntStatistics();
    stats4.setMinMax(0, 0);
    stats4.setNumNulls(304);
    b4 = makeBlockFromStats(stats4, 304);
    blocks.add(b4);


    IntStatistics stats5 = new IntStatistics();
    stats5.setMinMax(50, 50);
    stats5.setNumNulls(7);
    b5 = makeBlockFromStats(stats5, 305);
    blocks.add(b5);

    IntStatistics stats6 = new IntStatistics();
    stats6.setMinMax(0, 0);
    stats6.setNumNulls(12);
    b6 = makeBlockFromStats(stats6, 306);
    blocks.add(b6);

    schema = MessageTypeParser.parseMessageType("message Document { optional int32 foo; }");
    foo = intColumn("foo");
  }

  @Test
  public void testSkippedRowGroupFilters() {

    ParquetMetadata parquetMetadata = new ParquetMetadata(
        new FileMetaData(
            new MessageType("test", Types.required(PrimitiveType.PrimitiveTypeName.INT32).named
                ("field")),
            new HashMap<String, String>(),
            "parquet-mr version 1.6.0rc3 (build d4d5a07ec9bd262ca1e93c309f1d7d4a74ebda4c) "),
        blocks
    );

    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, 50)),
        parquetMetadata, schema);
    assertEquals(Arrays.asList(b1, b2, b3, b4, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(notEq(foo, 50)), parquetMetadata, schema);
    assertEquals(Arrays.asList(b1, b2, b3, b4, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, null)), parquetMetadata, schema);
    assertEquals(Arrays.asList(b1, b2, b3, b4, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(notEq(foo, null)), parquetMetadata, schema);
    assertEquals(Arrays.asList(b1, b2, b3, b4, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, 0)), parquetMetadata, schema);
    assertEquals(Arrays.asList(b1, b2, b3, b4, b5, b6), filtered);
  }

  @Test
  public void testAppliedRowGroupFilters() {

    ParquetMetadata parquetMetadata = new ParquetMetadata(
        new FileMetaData(
            new MessageType("test", Types.required(PrimitiveType.PrimitiveTypeName.INT32).named
                ("field")),
            new HashMap<String, String>(),
            "parquet-mr version 1.7.0rc3 (build d4d5a07ec9bd262ca1e93c309f1d7d4a74ebda4c) "),
        blocks
    );

    checkFilteredBlocks(parquetMetadata);
  }

  private void checkFilteredBlocks(ParquetMetadata parquetMetadata) {
    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, 50)),
        parquetMetadata, schema);
    assertEquals(Arrays.asList(b1, b2, b5), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(notEq(foo, 50)), parquetMetadata, schema);
    assertEquals(Arrays.asList(b1, b2, b3, b4, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, null)), parquetMetadata, schema);
    assertEquals(Arrays.asList(b1, b3, b4, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(notEq(foo, null)), parquetMetadata, schema);
    assertEquals(Arrays.asList(b1, b2, b3, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, 0)), parquetMetadata, schema);
    assertEquals(Arrays.asList(b6), filtered);
  }

}
