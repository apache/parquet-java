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
package org.apache.parquet.column.columnindex;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Formatter;
import java.util.List;
import java.util.Map;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveStringifier;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.booleans.BooleanLists;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;

/**
 * Builder implementation to create {@link ColumnIndex} objects during writing a parquet file.
 */
public abstract class ColumnIndexBuilder {

  static abstract class ColumnIndexBase implements ColumnIndex {
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
    private static final int MAX_VALUE_LENGTH_FOR_TOSTRING = 40;
    private static final String INNER_ETC = "(...)";
    private static final int FIRST_LENGTH = (MAX_VALUE_LENGTH_FOR_TOSTRING - INNER_ETC.length()) / 2;
    private static final int LAST_LENGTH = MAX_VALUE_LENGTH_FOR_TOSTRING - INNER_ETC.length() - FIRST_LENGTH;

    final PrimitiveStringifier stringifier;
    final PrimitiveComparator<Binary> comparator;
    private boolean[] nullPages;
    private BoundaryOrder boundaryOrder;
    // might be null
    private long[] nullCounts;

    static String truncate(String str) {
      if (str.length() <= MAX_VALUE_LENGTH_FOR_TOSTRING) {
        return str;
      }
      return str.substring(0, FIRST_LENGTH) + INNER_ETC + str.substring(str.length() - LAST_LENGTH);
    }

    ColumnIndexBase(PrimitiveType type) {
      comparator = type.comparator();
      stringifier = type.stringifier();
    }

    @Override
    public BoundaryOrder getBoundaryOrder() {
      return boundaryOrder;
    }

    @Override
    public List<Long> getNullCounts() {
      if (nullCounts == null) {
        return null;
      }
      return LongLists.unmodifiable(LongArrayList.wrap(nullCounts));
    }

    @Override
    public List<Boolean> getNullPages() {
      return BooleanLists.unmodifiable(BooleanArrayList.wrap(nullPages));
    }

    @Override
    public List<ByteBuffer> getMinValues() {
      List<ByteBuffer> list = new ArrayList<>(getPageCount());
      for (int i = 0, n = getPageCount(); i < n; ++i) {
        if (isNullPage(i)) {
          list.add(EMPTY_BYTE_BUFFER);
        } else {
          list.add(getMinValueAsBytes(i));
        }
      }
      return list;
    }

    @Override
    public List<ByteBuffer> getMaxValues() {
      List<ByteBuffer> list = new ArrayList<>(getPageCount());
      for (int i = 0, n = getPageCount(); i < n; ++i) {
        if (isNullPage(i)) {
          list.add(EMPTY_BYTE_BUFFER);
        } else {
          list.add(getMaxValueAsBytes(i));
        }
      }
      return list;
    }

    @Override
    public String toString() {
      try (Formatter formatter = new Formatter()) {
        formatter.format("Boudary order: %s\n", boundaryOrder);
        String minMaxPart = "  %-" + MAX_VALUE_LENGTH_FOR_TOSTRING + "s  %-" + MAX_VALUE_LENGTH_FOR_TOSTRING + "s\n";
        formatter.format("%-10s  %20s" + minMaxPart, "", "null count", "min", "max");
        String format = "page-%-5d  %20s" + minMaxPart;
        for (int i = 0, n = nullPages.length; i < n; ++i) {
          String nullCount = nullCounts == null ? "--" : Long.toString(nullCounts[i]);
          String min, max;
          if (nullPages[i]) {
            min = max = "--";
          } else {
            min = truncate(getMinValueAsString(i));
            max = truncate(getMaxValueAsString(i));
          }
          formatter.format(format, i, nullCount, min, max);
        }
        return formatter.toString();
      }
    }

    int getPageCount() {
      return nullPages.length;
    }

    boolean isNullPage(int pageIndex) {
      return nullPages[pageIndex];
    }

    abstract ByteBuffer getMinValueAsBytes(int pageIndex);

    abstract ByteBuffer getMaxValueAsBytes(int pageIndex);

    abstract String getMinValueAsString(int pageIndex);

    abstract String getMaxValueAsString(int pageIndex);
  }

  private static final ColumnIndexBuilder NO_OP_BUILDER = new ColumnIndexBuilder() {
    @Override
    public ColumnIndex build() {
      return null;
    }

    @Override
    public void add(Statistics<?> stats) {
    }

    @Override
    void addMinMax(Object min, Object max) {
    }

    @Override
    ColumnIndexBase createColumnIndex(PrimitiveType type) {
      return null;
    }

    @Override
    void clearMinMax() {
    }

    @Override
    void addMinMaxFromBytes(ByteBuffer min, ByteBuffer max) {
    }

    @Override
    int compareMinValues(PrimitiveComparator<Binary> comparator, int index1, int index2) {
      return 0;
    }

    @Override
    int compareMaxValues(PrimitiveComparator<Binary> comparator, int index1, int index2) {
      return 0;
    }
  };

  private static final Map<PrimitiveTypeName, ColumnIndexBuilder> BUILDERS = new EnumMap<>(PrimitiveTypeName.class);

  private PrimitiveType type;
  private final BooleanList nullPages = new BooleanArrayList();
  private final LongList nullCounts = new LongArrayList();

  /**
   * @return a no-op builder that does not collect statistics objects and therefore returns {@code null} at
   *         {@link #build()}.
   */
  public static ColumnIndexBuilder getNoOpBuilder() {
    return NO_OP_BUILDER;
  }

  /**
   * @param type
   *          the type this builder is to be created for
   * @return a {@link ColumnIndexBuilder} instance to be used for creating {@link ColumnIndex} objects
   */
  public static ColumnIndexBuilder getBuilder(PrimitiveType type) {
    ColumnIndexBuilder builder = createNewBuilder(type.getPrimitiveTypeName());
    builder.type = type;
    return builder;
  }

  private static ColumnIndexBuilder createNewBuilder(PrimitiveTypeName type) {
    switch (type) {
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
      case INT96:
        return new BinaryColumnIndexBuilder();
      case BOOLEAN:
        return new BooleanColumnIndexBuilder();
      case DOUBLE:
        return new DoubleColumnIndexBuilder();
      case FLOAT:
        return new FloatColumnIndexBuilder();
      case INT32:
        return new IntColumnIndexBuilder();
      case INT64:
        return new LongColumnIndexBuilder();
      default:
        throw new IllegalArgumentException("Unsupported type for column index: " + type);
    }
  }

  /**
   * @param type
   *          the primitive type
   * @param boundaryOrder
   *          the boundary order of the min/max values
   * @param nullPages
   *          the null pages
   * @param nullCounts
   *          the number of null values for each page
   * @param minValues
   *          the min values for each page
   * @param maxValues
   *          the max values for each page
   * @return the newly created {@link ColumnIndex} object based on the specified arguments
   */
  public static ColumnIndex build(
      PrimitiveType type,
      BoundaryOrder boundaryOrder,
      List<Boolean> nullPages,
      List<Long> nullCounts,
      List<ByteBuffer> minValues,
      List<ByteBuffer> maxValues) {

    PrimitiveTypeName typeName = type.getPrimitiveTypeName();
    ColumnIndexBuilder builder = BUILDERS.get(typeName);
    if (builder == null) {
      builder = createNewBuilder(typeName);
      BUILDERS.put(typeName, builder);
    }

    builder.fill(nullPages, nullCounts, minValues, maxValues);
    ColumnIndexBase columnIndex = builder.build(type);
    columnIndex.boundaryOrder = requireNonNull(boundaryOrder);
    return columnIndex;
  }

  ColumnIndexBuilder() {
    // Shall be able to be created inside this package only
  }

  /**
   * Adds the data from the specified statistics to this builder
   *
   * @param stats
   *          the statistics to be added
   */
  public void add(Statistics<?> stats) {
    if (stats.hasNonNullValue()) {
      nullPages.add(false);
      addMinMax(stats.genericGetMin(), stats.genericGetMax());
    } else {
      nullPages.add(true);
      addMinMax(null, null);
    }
    nullCounts.add(stats.getNumNulls());
  }

  abstract void addMinMaxFromBytes(ByteBuffer min, ByteBuffer max);

  abstract void addMinMax(Object min, Object max);

  private void fill(List<Boolean> nullPages, List<Long> nullCounts, List<ByteBuffer> minValues,
      List<ByteBuffer> maxValues) {
    clear();
    int requiredSize = nullPages.size();
    if ((nullCounts != null && nullCounts.size() != requiredSize) || minValues.size() != requiredSize
        || maxValues.size() != requiredSize) {
      throw new IllegalArgumentException(
          String.format("Not all sizes are equal (nullPages:%d, nullCounts:%s, minValues:%d, maxValues:%d",
              nullPages.size(), nullCounts == null ? "null" : nullCounts.size(), minValues.size(), maxValues.size()));
    }
    this.nullPages.addAll(nullPages);
    // Null counts is optional in the format
    if (nullCounts != null) {
      this.nullCounts.addAll(nullCounts);
    }

    for (int i = 0; i < requiredSize; ++i) {
      if (nullPages.get(i)) {
        addMinMaxFromBytes(null, null);
      } else {
        addMinMaxFromBytes(minValues.get(i), maxValues.get(i));
      }
    }
  }

  /**
   * Builds the column index. It also resets all the collected data.
   *
   * @return the newly created column index or {@code null} if the {@link ColumnIndex} would be empty
   */
  public ColumnIndex build() {
    ColumnIndexBase columnIndex = build(type);
    if (columnIndex == null) {
      return null;
    }
    columnIndex.boundaryOrder = calculateBoundaryOrder(type.comparator());
    clear();
    return columnIndex;
  }

  private ColumnIndexBase build(PrimitiveType type) {
    if (nullPages.isEmpty()) {
      return null;
    }
    ColumnIndexBase columnIndex = createColumnIndex(type);
    columnIndex.nullPages = nullPages.toBooleanArray();
    // Null counts is optional so keep it null if the builder has no values
    if (!nullCounts.isEmpty()) {
      columnIndex.nullCounts = nullCounts.toLongArray();
    }

    return columnIndex;
  }

  private BoundaryOrder calculateBoundaryOrder(PrimitiveComparator<Binary> comparator) {
    if (isAscending(comparator)) {
      return BoundaryOrder.ASCENDING;
    } else if (isDescending(comparator)) {
      return BoundaryOrder.DESCENDING;
    } else {
      return BoundaryOrder.UNORDERED;
    }
  }

  // min_i <= min_i+1 && max_i <= max_i+1
  private boolean isAscending(PrimitiveComparator<Binary> comparator) {
    int prevPage = nextNonNullPage(0);
    // All pages are null-page
    if (prevPage < 0) {
      return false;
    }
    int nextPage = nextNonNullPage(prevPage + 1);
    while (nextPage > 0) {
      if (compareMinValues(comparator, prevPage, nextPage) > 0
          || compareMaxValues(comparator, prevPage, nextPage) > 0) {
        return false;
      }
      prevPage = nextPage;
      nextPage = nextNonNullPage(nextPage + 1);
    }
    return true;
  }

  // min_i >= min_i+1 && max_i >= max_i+1
  private boolean isDescending(PrimitiveComparator<Binary> comparator) {
    int prevPage = nextNonNullPage(0);
    // All pages are null-page
    if (prevPage < 0) {
      return false;
    }
    int nextPage = nextNonNullPage(prevPage + 1);
    while (nextPage > 0) {
      if (compareMinValues(comparator, prevPage, nextPage) < 0
          || compareMaxValues(comparator, prevPage, nextPage) < 0) {
        return false;
      }
      prevPage = nextPage;
      nextPage = nextNonNullPage(nextPage + 1);
    }
    return true;
  }

  private int nextNonNullPage(int startIndex) {
    for (int i = startIndex, n = nullPages.size(); i < n; ++i) {
      if (!nullPages.get(i)) {
        return i;
      }
    }
    return -1;
  }

  abstract int compareMinValues(PrimitiveComparator<Binary> comparator, int index1, int index2);

  abstract int compareMaxValues(PrimitiveComparator<Binary> comparator, int index1, int index2);

  private void clear() {
    nullPages.clear();
    nullCounts.clear();
    clearMinMax();
  }

  abstract void clearMinMax();

  abstract ColumnIndexBase createColumnIndex(PrimitiveType type);
}
