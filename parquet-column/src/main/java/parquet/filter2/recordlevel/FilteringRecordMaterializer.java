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
package parquet.filter2.recordlevel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parquet.common.schema.ColumnPath;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import parquet.io.PrimitiveColumnIO;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;

import static parquet.Preconditions.checkNotNull;

/**
 * A pass-through proxy for a {@link RecordMaterializer} that updates a {@link IncrementallyUpdatedFilterPredicate}
 * as it receives concrete values for the current record. If, after the record assembly signals that
 * there are no more values, the predicate indicates that this record should be dropped, {@link #getCurrentRecord()}
 * returns null to signal that this record is being skipped.
 * Otherwise, the record is retrieved from the delegate.
 */
public class FilteringRecordMaterializer<T> extends RecordMaterializer<T> {
  // the real record materializer
  private final RecordMaterializer<T> delegate;

  // the proxied root converter
  private final FilteringGroupConverter rootConverter;

  // the predicate
  private final IncrementallyUpdatedFilterPredicate filterPredicate;

  public FilteringRecordMaterializer(
      RecordMaterializer<T> delegate,
      List<PrimitiveColumnIO> columnIOs,
      Map<ColumnPath, List<ValueInspector>> valueInspectorsByColumn,
      IncrementallyUpdatedFilterPredicate filterPredicate) {

    checkNotNull(columnIOs, "columnIOs");
    checkNotNull(valueInspectorsByColumn, "valueInspectorsByColumn");
    this.filterPredicate = checkNotNull(filterPredicate, "filterPredicate");
    this.delegate = checkNotNull(delegate, "delegate");

    // keep track of which path of indices leads to which primitive column
    Map<List<Integer>, PrimitiveColumnIO> columnIOsByIndexFieldPath = new HashMap<List<Integer>, PrimitiveColumnIO>();

    for (PrimitiveColumnIO c : columnIOs) {
      columnIOsByIndexFieldPath.put(getIndexFieldPathList(c), c);
    }

    // create a proxy for the delegate's root converter
    this.rootConverter = new FilteringGroupConverter(
        delegate.getRootConverter(), Collections.<Integer>emptyList(), valueInspectorsByColumn, columnIOsByIndexFieldPath);
  }

  public static List<Integer> getIndexFieldPathList(PrimitiveColumnIO c) {
    return intArrayToList(c.getIndexFieldPath());
  }

  public static List<Integer> intArrayToList(int[] arr) {
    List<Integer> list = new ArrayList<Integer>(arr.length);
    for (int i : arr) {
      list.add(i);
    }
    return list;
  }



  @Override
  public T getCurrentRecord() {

    // find out if the predicate thinks we should keep this record
    boolean keep = IncrementallyUpdatedFilterPredicateEvaluator.evaluate(filterPredicate);

    // reset the stateful predicate no matter what
    IncrementallyUpdatedFilterPredicateResetter.reset(filterPredicate);

    if (keep) {
      return delegate.getCurrentRecord();
    } else {
      // signals a skip
      return null;
    }
  }

  @Override
  public void skipCurrentRecord() {
    delegate.skipCurrentRecord();
  }

  @Override
  public GroupConverter getRootConverter() {
    return rootConverter;
  }
}
