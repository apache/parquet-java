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
import java.util.List;

import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter;
import org.apache.parquet.filter2.compat.FilterCompat.Visitor;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.SchemaCompatibilityValidator;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import static org.apache.parquet.Preconditions.checkNotNull;

/**
 * Given a {@link Filter} applies it to a list of BlockMetaData (row groups)
 * If the Filter is an {@link org.apache.parquet.filter.UnboundRecordFilter} or the no op filter,
 * no filtering will be performed.
 */
public class RowGroupFilter implements Visitor<List<BlockMetaData>> {
  private static final int STATISTICS_FIXED_VERSION = 161; // 1.6.1
  private final List<BlockMetaData> blocks;
  private final MessageType schema;
  private final Boolean skipStatisticsChecks;

  public static List<BlockMetaData> filterRowGroups(Filter filter, ParquetMetadata footer,
                                                    MessageType schema) {
    checkNotNull(filter, "filter");
    return filter.accept(new RowGroupFilter(footer, schema));
  }

  private RowGroupFilter(ParquetMetadata footer, MessageType schema) {
    String createdBy = checkNotNull(footer.getFileMetaData().getCreatedBy(), "createdBy");
    this.skipStatisticsChecks = shouldIgnoreStatistics(createdBy);
    this.blocks = checkNotNull(footer.getBlocks(), "blocks");
    this.schema = checkNotNull(schema, "schema");
  }

  private static boolean shouldIgnoreStatistics(String createdBy) {
    final String[] tokens = createdBy.split(" ");
    final String app = tokens[0];
    final String version = tokens[2].substring(0, 5).replaceAll("\\.", "");

    if (app.equalsIgnoreCase("parquet-mr")) {
      return Integer.parseInt(version) < STATISTICS_FIXED_VERSION;
    }

    return true;
  }

  @Override
  public List<BlockMetaData> visit(FilterCompat.FilterPredicateCompat filterPredicateCompat) {
    if (skipStatisticsChecks) {
      return blocks;
    }

    FilterPredicate filterPredicate = filterPredicateCompat.getFilterPredicate();

    // check that the schema of the filter matches the schema of the file
    SchemaCompatibilityValidator.validate(filterPredicate, schema);

    List<BlockMetaData> filteredBlocks = new ArrayList<BlockMetaData>();

    for (BlockMetaData block : blocks) {
      if (!StatisticsFilter.canDrop(filterPredicate, block.getColumns())) {
        filteredBlocks.add(block);
      }
    }

    return filteredBlocks;
  }

  @Override
  public List<BlockMetaData> visit(FilterCompat.UnboundRecordFilterCompat unboundRecordFilterCompat) {
    return blocks;
  }

  @Override
  public List<BlockMetaData> visit(NoOpFilter noOpFilter) {
    return blocks;
  }
}
