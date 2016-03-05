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
import java.util.List;

import org.apache.parquet.column.statistics.ColumnStatisticsOpts;
import org.apache.parquet.column.statistics.StatisticsOpts;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterOptBuilder;
import org.apache.parquet.column.statistics.bloomfilter.BloomFilterOpts;
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
  @Test
  public void testApplyRowGroupFilters() {

    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

    IntStatistics stats1 = new IntStatistics(new ColumnStatisticsOpts(null));
    stats1.setMinMax(10, 100);
    stats1.setNumNulls(4);
    BlockMetaData b1 = makeBlockFromStats(stats1, 301);
    blocks.add(b1);

    IntStatistics stats2 = new IntStatistics(new ColumnStatisticsOpts(null));
    stats2.setMinMax(8, 102);
    stats2.setNumNulls(0);
    BlockMetaData b2 = makeBlockFromStats(stats2, 302);
    blocks.add(b2);

    IntStatistics stats3 = new IntStatistics(new ColumnStatisticsOpts(null));
    stats3.setMinMax(100, 102);
    stats3.setNumNulls(12);
    BlockMetaData b3 = makeBlockFromStats(stats3, 303);
    blocks.add(b3);


    IntStatistics stats4 = new IntStatistics(new ColumnStatisticsOpts(null));
    stats4.setMinMax(0, 0);
    stats4.setNumNulls(304);
    BlockMetaData b4 = makeBlockFromStats(stats4, 304);
    blocks.add(b4);

    IntStatistics stats5 = new IntStatistics(new ColumnStatisticsOpts(null));
    stats5.setMinMax(50, 50);
    stats5.setNumNulls(7);
    BlockMetaData b5 = makeBlockFromStats(stats5, 305);
    blocks.add(b5);

    IntStatistics stats6 = new IntStatistics(new ColumnStatisticsOpts(null));
    stats6.setMinMax(0, 0);
    stats6.setNumNulls(12);
    BlockMetaData b6 = makeBlockFromStats(stats6, 306);
    blocks.add(b6);

    MessageType schema = MessageTypeParser.parseMessageType("message Document { optional int32 foo; }");
    IntColumn foo = intColumn("foo");

    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, 50)), blocks, schema);
    assertEquals(Arrays.asList(b1, b2, b5), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(notEq(foo, 50)), blocks, schema);
    assertEquals(Arrays.asList(b1, b2, b3, b4, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, null)), blocks, schema);
    assertEquals(Arrays.asList(b1, b3, b4, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(notEq(foo, null)), blocks, schema);
    assertEquals(Arrays.asList(b1, b2, b3, b5, b6), filtered);

    filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, 0)), blocks, schema);
    assertEquals(Arrays.asList(b6), filtered);
  }

  @Test
  public void testApplyRowGroupFiltersWithBloomFilter() {

    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

    MessageType schema =
        MessageTypeParser.parseMessageType("message Document { optional int32 foo; }");
    BloomFilterOpts opts =
        new BloomFilterOptBuilder().enableCols("foo").expectedEntries("1000").build(schema);
    ColumnStatisticsOpts columnStatisticsOpts =
        new StatisticsOpts(opts).getStatistics(schema.getColumnDescription(new String[] { "foo" }));
    IntStatistics stats1 = new IntStatistics(columnStatisticsOpts);
    stats1.setMinMax(10, 100);
    stats1.setNumNulls(4);
    stats1.add(25);
    stats1.add(30);
    stats1.add(43);
    BlockMetaData b1 = makeBlockFromStats(stats1, 301);
    blocks.add(b1);

    IntStatistics stats2 = new IntStatistics(columnStatisticsOpts);
    stats2.setMinMax(8, 102);
    stats2.setNumNulls(0);
    stats2.add(12);
    stats2.add(30);
    stats2.add(90);
    BlockMetaData b2 = makeBlockFromStats(stats2, 302);
    blocks.add(b2);

    IntStatistics stats3 = new IntStatistics(columnStatisticsOpts);
    stats3.setMinMax(3, 90);
    stats3.setNumNulls(12);
    stats3.add(20);
    stats1.add(40);
    BlockMetaData b3 = makeBlockFromStats(stats3, 303);
    blocks.add(b3);


    IntColumn foo = intColumn("foo");

    List<BlockMetaData> filtered = RowGroupFilter.filterRowGroups(FilterCompat.get(eq(foo, 30)), blocks, schema);
    assertEquals(Arrays.asList(b1, b2), filtered);
  }
}
