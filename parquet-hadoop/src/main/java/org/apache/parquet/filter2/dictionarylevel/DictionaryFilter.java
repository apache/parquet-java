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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.Log;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.*;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.io.IOException;
import java.util.*;

import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.Preconditions.checkNotNull;


/**
 * Applies filters based on the contents of column dictionaries.
 */
public class DictionaryFilter implements FilterPredicate.Visitor<Boolean> {

  private static final Log LOG = Log.getLog(DictionaryFilter.class);

  public static boolean canDrop(FilterPredicate pred, List<ColumnChunkMetaData> columns, FSDataInputStream s) {
    checkNotNull(pred, "pred");
    checkNotNull(columns, "columns");
    return pred.accept(new DictionaryFilter(columns, s));
  }

  private Configuration conf = null;
  private final Map<ColumnPath, ColumnChunkMetaData> columns = new HashMap<ColumnPath, ColumnChunkMetaData>();
  private FSDataInputStream f;

  private DictionaryFilter(List<ColumnChunkMetaData> columnsList, FSDataInputStream s) {
    for (ColumnChunkMetaData chunk : columnsList) {
      columns.put(chunk.getPath(), chunk);
    }

    this.conf = new Configuration();
    this.f = s;
  }

  private ColumnChunkMetaData getColumnChunk(ColumnPath columnPath) {
    ColumnChunkMetaData c = columns.get(columnPath);
    checkArgument(c != null, "Column " + columnPath.toDotString() + " not found in schema!");
    return c;
  }

  private <T extends Comparable<T>> Set<T> expandDictionary(ColumnChunkMetaData meta) throws IOException, ParquetRuntimeException {
    DictionaryPage page = ParquetFileReader.getDictionary(conf, meta, f);

    Dictionary dict = page.getEncoding().initDictionary(new ColumnDescriptor(null, meta.getType(), -1, -1), page);

    Set dictSet = new HashSet();

    for(int i=0; i<dict.getMaxId(); i++) {
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

    return dictSet;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
    Column<T> filterColumn = eq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());
    T value = eq.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);
      return !dictSet.contains(value);
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return false;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
    Column<T> filterColumn = notEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());
    T value = notEq.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);
      return dictSet.size() == 1 && dictSet.contains(value);
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return false;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Lt<T> lt) {
    Column<T> filterColumn = lt.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());
    T value = lt.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);

      for(T entry : dictSet) {
        if(value.compareTo(entry) > 0) {
          return false;
        }
      }

      return true;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return false;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
    Column<T> filterColumn = ltEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());
    T value = ltEq.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);

      for(T entry : dictSet) {
        if(value.compareTo(entry) >= 0) {
          return false;
        }
      }

      return true;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return false;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Gt<T> gt) {
    Column<T> filterColumn = gt.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());
    T value = gt.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);

      for(T entry : dictSet) {
        if(value.compareTo(entry) < 0) {
          return false;
        }
      }

      return true;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return false;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
    Column<T> filterColumn = gtEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());
    T value = gtEq.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);

      for(T entry : dictSet) {
        if(value.compareTo(entry) <= 0) {
          return false;
        }
      }

      return true;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return false;
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

}
