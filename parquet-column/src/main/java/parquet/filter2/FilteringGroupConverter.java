package parquet.filter2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import parquet.ColumnPath;
import parquet.filter2.StreamingFilterPredicate.Atom;
import parquet.io.PrimitiveColumnIO;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;

public class FilteringGroupConverter extends GroupConverter {
  private final GroupConverter delegate;
  private final List<Integer> indexFieldPath;
  private final Map<ColumnPath, List<Atom>> atomsByColumn;
  private final Map<List<Integer>, PrimitiveColumnIO> columnIOsByIndexFieldPath;

  public FilteringGroupConverter(
      GroupConverter delegate,
      List<Integer> indexFieldPath,
      Map<ColumnPath, List<Atom>> atomsByColumn, Map<List<Integer>,
      PrimitiveColumnIO> columnIOsByIndexFieldPath) {

    this.delegate = delegate;
    this.indexFieldPath = indexFieldPath;
    this.columnIOsByIndexFieldPath = columnIOsByIndexFieldPath;
    this.atomsByColumn = atomsByColumn;
  }

  // TODO: making the assumption that getConverter(i) is only called once, is that valid?
  @Override
  public Converter getConverter(int fieldIndex) {
    List<Integer> newIndexFieldPath = new ArrayList<Integer>(indexFieldPath.size() + 1);
    newIndexFieldPath.addAll(indexFieldPath);
    newIndexFieldPath.add(fieldIndex);

    Converter delegateConverter = delegate.getConverter(fieldIndex);

    if (delegateConverter.isPrimitive()) {
      PrimitiveColumnIO columnIO = columnIOsByIndexFieldPath.get(newIndexFieldPath);
      ColumnPath columnPath = ColumnPath.get(columnIO.getColumnDescriptor().getPath());
      Atom[] atoms;
      List<Atom> atomsList = atomsByColumn.get(columnPath);
      if (atomsList == null) {
        atoms = new Atom[] {};
      } else {
        atoms = (Atom[]) atomsList.toArray();
      }
      return new AtomUpdatingConverter(delegateConverter.asPrimitiveConverter(), atoms);
    } else {
      return new FilteringGroupConverter(delegateConverter.asGroupConverter(), newIndexFieldPath, atomsByColumn, columnIOsByIndexFieldPath);
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
