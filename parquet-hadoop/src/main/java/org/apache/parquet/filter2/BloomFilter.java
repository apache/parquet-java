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
package org.apache.parquet.filter2;


import org.apache.parquet.column.values.bloom.Bloom;
import org.apache.parquet.column.values.bloom.BloomDataReadStore;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.Preconditions.checkNotNull;

public class BloomFilter implements FilterPredicate.Visitor<Boolean>{
  private static final boolean BLOCK_MIGHT_MATCH = false;
  private static final boolean BLOCK_CANNOT_MATCH = true;

  private final Map<ColumnPath, ColumnChunkMetaData> columns = new HashMap<ColumnPath, ColumnChunkMetaData>();

  public static boolean canDrop(FilterPredicate pred, List<ColumnChunkMetaData> columns, BloomDataReadStore bloomFilterReader) {
    checkNotNull(pred, "pred");
    checkNotNull(columns, "columns");
    return pred.accept(new BloomFilter(columns, bloomFilterReader));
  }

  private BloomFilter(List<ColumnChunkMetaData> columnsList, BloomDataReadStore bloomFilterReader) {
    for (ColumnChunkMetaData chunk : columnsList) {
      columns.put(chunk.getPath(), chunk);
    }

    this.bloomFilterReader = bloomFilterReader;
  }

  private BloomDataReadStore bloomFilterReader;

  private ColumnChunkMetaData getColumnChunk(ColumnPath columnPath) {
    return columns.get(columnPath);
  }

  // is this column chunk composed entirely of nulls?
  // assumes the column chunk's statistics is not empty
  private boolean isAllNulls(ColumnChunkMetaData column) {
    return column.getStatistics().getNumNulls() == column.getValueCount();
  }

  // are there any nulls in this column chunk?
  // assumes the column chunk's statistics is not empty
  private boolean hasNulls(ColumnChunkMetaData column) {
    return column.getStatistics().getNumNulls() > 0;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.Eq<T> eq) {
    T value = eq.getValue();

    if (value == null) {
      // the bloom filter bitset contains only non-null values so isn't helpful. this
      // could check the column stats, but the StatisticsFilter is responsible
      return BLOCK_MIGHT_MATCH;
    }

    Operators.Column<T> filterColumn = eq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());
    ColumnDescriptor col = new ColumnDescriptor(meta.getPath().toArray(), meta.getType(), -1, -1);
    if (meta == null) {
      // the column isn't in this file so all values are null, but the value
      // must be non-null because of the above check.
      return BLOCK_CANNOT_MATCH;
    }

    Bloom bloom = bloomFilterReader.readBloomData(col);
    if (bloom != null && bloom.find(value) == false) {
      return BLOCK_CANNOT_MATCH;
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.NotEq<T> notEq) {
    Operators.Column<T> filterColumn = notEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    T value = notEq.getValue();

    if (meta == null) {
      if (value == null) {
        // null is always equal to null
        return BLOCK_CANNOT_MATCH;
      }
      return BLOCK_MIGHT_MATCH;
    }

    if (value == null) {
      // we are looking for records where v notEq(null)
      // so, if this is a column of all nulls, we can drop it
      return isAllNulls(meta);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.Lt<T> lt) {
    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.LtEq<T> ltEq) {
    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.Gt<T> gt) {
    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.GtEq<T> gtEq) {
    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public Boolean visit(Operators.And and) {
    return and.getLeft().accept(this) || and.getRight().accept(this);
  }

  @Override
  public Boolean visit(Operators.Or or) {
    return or.getLeft().accept(this) && or.getRight().accept(this);
  }

  @Override
  public Boolean visit(Operators.Not not) {
    throw new IllegalArgumentException(
      "This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter? " + not);
  }


  private <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(Operators.UserDefined<T, U> ud, boolean inverted) {
    Operators.Column<T> filterColumn = ud.getColumn();
    ColumnChunkMetaData columnChunk = getColumnChunk(filterColumn.getColumnPath());
    U udp = ud.getUserDefinedPredicate();

    if (columnChunk == null) {
      // the column isn't in this file so all values are null.
      // lets run the udp with null value to see if it keeps null or not.
      if (inverted) {
        return udp.keep(null);
      } else {
        return !udp.keep(null);
      }
    }

    if (isAllNulls(columnChunk)) {
      // lets run the udp with null value to see if it keeps null or not.
      if (inverted) {
        return udp.keep(null);
      } else {
        return !udp.keep(null);
      }
    }

     return BLOCK_MIGHT_MATCH;
  }


  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(Operators.UserDefined<T, U> udp) {
    return visit(udp, false);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(Operators.LogicalNotUserDefined<T, U> udp) {
    return visit(udp.getUserDefined(), true);
  }
}
