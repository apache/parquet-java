package parquet.filter2.recordlevel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parquet.ColumnPath;
import parquet.filter2.recordlevel.StreamingFilterPredicate.Atom;
import parquet.io.PrimitiveColumnIO;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;

/**
 * A pass through proxy for a {@link RecordMaterializer} that updates a {@link StreamingFilterPredicate}
 * as it receives concrete values for the current record. If, after the record assembly signals that
 * there are no more values, the predicate indicates that this record should be filtered, {@link #getCurrentRecord()}
 * returns null to signal that this record is being skipped.
 * Otherwise, the record is retrieved from the proxy delegate.
 */
public class FilteringRecordMaterializer<T> extends RecordMaterializer<T> {
  // the real record materializer
  private final RecordMaterializer<T> delegate;

  // the proxied root converter
  private final FilteringGroupConverter rootConverter;

  // the predicate
  private final StreamingFilterPredicate filterPredicate;

  public FilteringRecordMaterializer(
      RecordMaterializer<T> delegate,
      PrimitiveColumnIO[] columnIOs,
      Map<ColumnPath, List<Atom>> atomsByColumn,
      StreamingFilterPredicate filterPredicate) {

    // keep track of which path if indexes leads to which primitive column
    Map<List<Integer>, PrimitiveColumnIO> columnIOsByIndexFieldPath = new HashMap<List<Integer>, PrimitiveColumnIO>();

    for (PrimitiveColumnIO c : columnIOs) {
      columnIOsByIndexFieldPath.put(getIndexFieldPathList(c), c);
    }

    this.filterPredicate = filterPredicate;
    this.delegate = delegate;

    // create a proxy for the delegate's root converter
    this.rootConverter = new FilteringGroupConverter(
        delegate.getRootConverter(), Collections.<Integer>emptyList(), atomsByColumn, columnIOsByIndexFieldPath);
  }

  public static List<Integer> getIndexFieldPathList(PrimitiveColumnIO c) {
    return intArrayToList(c.getIndexFieldPath());
  }

  public static List<Integer> intArrayToList(int [] arr) {
    List<Integer> list = new ArrayList<Integer>(arr.length);
    for (int i : arr) {
      list.add(i);
    }
    return list;
  }

  @Override
  public T getCurrentRecord() {

    // find out if the predicate thinks we should keep this record
    boolean keep = StreamingFilterPredicateEvaluator.evaluate(filterPredicate);

    // reset the stateful predicate
    StreamingFilterPredicateReseter.reset(filterPredicate);

    if (keep) {
      return delegate.getCurrentRecord();
    } else {
      // signals a skip
      return null;
    }
  }

  @Override
  public GroupConverter getRootConverter() {
    return rootConverter;
  }
}
