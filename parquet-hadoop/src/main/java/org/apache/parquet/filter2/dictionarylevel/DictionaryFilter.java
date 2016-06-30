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
package org.apache.parquet.filter2.dictionarylevel;

import org.apache.parquet.Log;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.*;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.Preconditions.checkNotNull;


/**
 * Applies filters based on the contents of column dictionaries.
 */
public class DictionaryFilter implements FilterPredicate.Visitor<Boolean> {

  private static final Log LOG = Log.getLog(DictionaryFilter.class);
  private static final boolean BLOCK_MIGHT_MATCH = false;
  private static final boolean BLOCK_CANNOT_MATCH = true;

  public static boolean canDrop(FilterPredicate pred, List<ColumnChunkMetaData> columns, DictionaryPageReadStore dictionaries) {
    checkNotNull(pred, "pred");
    checkNotNull(columns, "columns");
    return pred.accept(new DictionaryFilter(columns, dictionaries));
  }

  private final Map<ColumnPath, ColumnChunkMetaData> columns = new HashMap<ColumnPath, ColumnChunkMetaData>();
  private final DictionaryPageReadStore dictionaries;

  private DictionaryFilter(List<ColumnChunkMetaData> columnsList, DictionaryPageReadStore dictionaries) {
    for (ColumnChunkMetaData chunk : columnsList) {
      columns.put(chunk.getPath(), chunk);
    }

    this.dictionaries = dictionaries;
  }

  private ColumnChunkMetaData getColumnChunk(ColumnPath columnPath) {
    ColumnChunkMetaData c = columns.get(columnPath);
    checkArgument(c != null, "Column " + columnPath.toDotString() + " not found in schema!");
    return c;
  }

  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> Set<T> expandDictionary(ColumnChunkMetaData meta) throws IOException {
    ColumnDescriptor col = new ColumnDescriptor(meta.getPath().toArray(), meta.getType(), -1, -1);
    DictionaryPage page = dictionaries.readDictionaryPage(col);

    // the chunk may not be dictionary-encoded
    if (page == null) {
      return null;
    }

    Dictionary dict = page.getEncoding().initDictionary(col, page);

    Set dictSet = new HashSet<T>();

    for (int i=0; i<=dict.getMaxId(); i++) {
      switch(meta.getType()) {
        case BINARY: dictSet.add(dict.decodeToBinary(i));
          break;
        case INT32: dictSet.add(dict.decodeToInt(i));
          break;
        case INT64: dictSet.add(dict.decodeToLong(i));
          break;
        case FLOAT: dictSet.add(dict.decodeToFloat(i));
          break;
        case DOUBLE: dictSet.add(dict.decodeToDouble(i));
          break;
        default:
          LOG.warn("Unknown dictionary type" + meta.getType());
      }
    }

    return (Set<T>) dictSet;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
    Column<T> filterColumn = eq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = eq.getValue();

    filterColumn.getColumnPath();

    if (value == null) {
      // the dictionary contains only non-null values so isn't helpful. this
      // could check the column stats, but the StatisticsFilter is responsible
      return BLOCK_MIGHT_MATCH;
    }

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet != null && !dictSet.contains(value)) {
        return BLOCK_CANNOT_MATCH;
      }
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH; // cannot drop the row group based on this dictionary
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
    Column<T> filterColumn = notEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = notEq.getValue();

    filterColumn.getColumnPath();

    if (value == null) {
      // the dictionary contains only non-null values so isn't helpful. this
      // could check the column stats, but the StatisticsFilter is responsible
      return BLOCK_MIGHT_MATCH;
    }

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet != null && dictSet.size() == 1 && dictSet.contains(value)) {
        return BLOCK_CANNOT_MATCH;
      }
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Lt<T> lt) {
    Column<T> filterColumn = lt.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = lt.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet == null) {
        return BLOCK_MIGHT_MATCH;
      }

      for(T entry : dictSet) {
        if(value.compareTo(entry) > 0) {
          return BLOCK_MIGHT_MATCH;
        }
      }

      return BLOCK_CANNOT_MATCH;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
    Column<T> filterColumn = ltEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = ltEq.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet == null) {
        return BLOCK_MIGHT_MATCH;
      }

      for(T entry : dictSet) {
        if(value.compareTo(entry) >= 0) {
          return BLOCK_MIGHT_MATCH;
        }
      }

      return BLOCK_CANNOT_MATCH;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Gt<T> gt) {
    Column<T> filterColumn = gt.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = gt.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet == null) {
        return BLOCK_MIGHT_MATCH;
      }

      for(T entry : dictSet) {
        if(value.compareTo(entry) < 0) {
          return BLOCK_MIGHT_MATCH;
        }
      }

      return BLOCK_CANNOT_MATCH;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
    Column<T> filterColumn = gtEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = gtEq.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet == null) {
        return BLOCK_MIGHT_MATCH;
      }

      for(T entry : dictSet) {
        if(value.compareTo(entry) <= 0) {
          return BLOCK_MIGHT_MATCH;
        }
      }

      return BLOCK_CANNOT_MATCH;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public Boolean visit(And and) {
    return and.getLeft().accept(this) || and.getRight().accept(this);
  }

  @Override
  public Boolean visit(Or or) {
    return or.getLeft().accept(this) && or.getRight().accept(this);
  }

  @Override
  public Boolean visit(Not not) {
    throw new IllegalArgumentException(
        "This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter? " + not);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(UserDefined<T, U> udp) {
    throw new UnsupportedOperationException("UDP not supported with dictionary evaluation.");
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(LogicalNotUserDefined<T, U> udp) {
    throw new UnsupportedOperationException("UDP not supported with dictionary evaluation.");
  }

  @SuppressWarnings("deprecation")
  private static boolean hasNonDictionaryPages(ColumnChunkMetaData meta) {
    EncodingStats stats = meta.getEncodingStats();
    if (stats != null) {
      return stats.hasNonDictionaryEncodedPages();
    }

    // without EncodingStats, fall back to testing the encoding list
    Set<Encoding> encodings = new HashSet<Encoding>(meta.getEncodings());
    if (encodings.remove(Encoding.PLAIN_DICTIONARY)) {
      // if remove returned true, PLAIN_DICTIONARY was present, which means at
      // least one page was dictionary encoded and 1.0 encodings are used

      // RLE and BIT_PACKED are only used for repetition or definition levels
      encodings.remove(Encoding.RLE);
      encodings.remove(Encoding.BIT_PACKED);

      if (encodings.isEmpty()) {
        return false; // no encodings other than dictionary or rep/def levels
      }

      return true;

    } else {
      // if PLAIN_DICTIONARY wasn't present, then either the column is not
      // dictionary-encoded, or the 2.0 encoding, RLE_DICTIONARY, was used.
      // for 2.0, this cannot determine whether a page fell back without
      // page encoding stats
      return true;
    }
  }
}
