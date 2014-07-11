package parquet.filter2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parquet.ColumnPath;
import parquet.filter2.StreamingFilterPredicate.Atom;
import parquet.io.PrimitiveColumnIO;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;

public class FilteringRecordMaterializer<T> extends RecordMaterializer<T> {
  private final RecordMaterializer<T> delegate;
  private final FilteringGroupConverter rootConverter;
  private final StreamingFilterPredicate filterPredicate;

  public FilteringRecordMaterializer(
      RecordMaterializer<T> delegate,
      PrimitiveColumnIO[] columnIOs,
      Map<ColumnPath, List<Atom>> atomsByColumn,
      StreamingFilterPredicate filterPredicate) {

    Map<List<Integer>, PrimitiveColumnIO> columnIOsByIndexFieldPath = new HashMap<List<Integer>, PrimitiveColumnIO>();

    for (PrimitiveColumnIO c : columnIOs) {
      columnIOsByIndexFieldPath.put(getIndexFieldPathList(c), c);
    }

    this.filterPredicate = filterPredicate;
    this.delegate = delegate;
    this.rootConverter = new FilteringGroupConverter(
        delegate.getRootConverter(), Arrays.asList(0), atomsByColumn, columnIOsByIndexFieldPath);
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
    boolean keep = StreamingFilterPredicateEvaluator.evaluate(filterPredicate);
    StreamingFilterPredicateReseter.reset(filterPredicate);

    if (keep) {
      return delegate.getCurrentRecord();
    } else {
      return null;
    }
  }

  @Override
  public GroupConverter getRootConverter() {
    return rootConverter;
  }
}
