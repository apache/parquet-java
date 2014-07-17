package parquet.filter2.recordlevel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import parquet.ColumnPath;
import parquet.Preconditions;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import parquet.io.PrimitiveColumnIO;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;

/**
 * See {@link FilteringRecordMaterializer}
 */
public class FilteringGroupConverter extends GroupConverter {
  // the real converter
  private final GroupConverter delegate;

  // the path, from the root of the schema, to this converter
  // used ultimately by the primitive converter proxy to figure
  // out which column it represents.
  private final List<Integer> indexFieldPath;

  // for a given column, which nodes in the filter expression need
  // to be notified of this column's value
  private final Map<ColumnPath, List<ValueInspector>> valueInspectorsByColumn;

  // used to go from our indexFieldPath to the PrimitiveColumnIO for that column
  private final Map<List<Integer>, PrimitiveColumnIO> columnIOsByIndexFieldPath;

  public FilteringGroupConverter(
      GroupConverter delegate,
      List<Integer> indexFieldPath,
      Map<ColumnPath, List<ValueInspector>> valueInspectorsByColumn, Map<List<Integer>,
      PrimitiveColumnIO> columnIOsByIndexFieldPath) {

    this.delegate = Preconditions.checkNotNull(delegate, "delegate");
    this.indexFieldPath = Preconditions.checkNotNull(indexFieldPath, "indexFieldPath");
    this.columnIOsByIndexFieldPath = Preconditions.checkNotNull(columnIOsByIndexFieldPath, "columnIOsByIndexFieldPath");
    this.valueInspectorsByColumn = Preconditions.checkNotNull(valueInspectorsByColumn, "valueInspectorsByColumn");
  }

  // When a converter is asked for, we get the real one from the delegate, then wrap it
  // in a filtering pass-through proxy.
  // TODO: making the assumption that getConverter(i) is only called once, is that valid?
  @Override
  public Converter getConverter(int fieldIndex) {

    // get the real converter from the delegate
    Converter delegateConverter = Preconditions.checkNotNull(delegate.getConverter(fieldIndex), "delegate converter");

    // determine the indexFieldPath for the converter proxy we're about to make, which is
    // this converter's path + the requested fieldIndex
    List<Integer> newIndexFieldPath = new ArrayList<Integer>(indexFieldPath.size() + 1);
    newIndexFieldPath.addAll(indexFieldPath);
    newIndexFieldPath.add(fieldIndex);

    if (delegateConverter.isPrimitive()) {
      PrimitiveColumnIO columnIO = getColumnIO(newIndexFieldPath);
      ColumnPath columnPath = ColumnPath.get(columnIO.getColumnDescriptor().getPath());
      ValueInspector[] valueInspectors = getValueInspectors(columnPath);
      return new FilteringPrimitiveConverter(delegateConverter.asPrimitiveConverter(), valueInspectors);
    } else {
      return new FilteringGroupConverter(delegateConverter.asGroupConverter(), newIndexFieldPath, valueInspectorsByColumn, columnIOsByIndexFieldPath);
    }

  }

  private PrimitiveColumnIO getColumnIO(List<Integer> indexFieldPath) {
    PrimitiveColumnIO found = columnIOsByIndexFieldPath.get(indexFieldPath);
    Preconditions.checkArgument(found != null, "Did not find PrimitiveColumnIO for index field path" + indexFieldPath);
    return found;
  }

  private ValueInspector[] getValueInspectors(ColumnPath columnPath) {
    List<ValueInspector> inspectorsList = valueInspectorsByColumn.get(columnPath);
    if (inspectorsList == null) {
      return new ValueInspector[] {};
    } else {
      return inspectorsList.toArray(new ValueInspector[inspectorsList.size()]);
    }
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  public void end() {
    delegate.end();
  }
}
