package parquet.filter2.recordlevel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import parquet.ColumnPath;
import parquet.filter2.recordlevel.StreamingFilterPredicate.Atom;
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
  private final Map<ColumnPath, List<Atom>> atomsByColumn;

  // used to go from our indexFieldPath to the PrimitiveColumnIO for that column
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

  // When a converter is asked for, we get the real one from the delegate, then wrap it
  // in a filtering pass through proxy.
  // TODO: making the assumption that getConverter(i) is only called once, is that valid?
  @Override
  public Converter getConverter(int fieldIndex) {

    // get the real converter from the delegate
    Converter delegateConverter = delegate.getConverter(fieldIndex);

    // determine the indexFieldPath for the converter proxy we're about to make, which is
    // this converter's path + the requested fieldIndex
    List<Integer> newIndexFieldPath = new ArrayList<Integer>(indexFieldPath.size() + 1);
    newIndexFieldPath.addAll(indexFieldPath);
    newIndexFieldPath.add(fieldIndex);

    if (delegateConverter.isPrimitive()) {
      PrimitiveColumnIO columnIO = columnIOsByIndexFieldPath.get(newIndexFieldPath);
      ColumnPath columnPath = ColumnPath.get(columnIO.getColumnDescriptor().getPath());
      Atom[] atoms = getAtoms(columnPath);
      return new AtomUpdatingConverter(delegateConverter.asPrimitiveConverter(), atoms);
    } else {
      return new FilteringGroupConverter(delegateConverter.asGroupConverter(), newIndexFieldPath, atomsByColumn, columnIOsByIndexFieldPath);
    }

  }

  private Atom[] getAtoms(ColumnPath columnPath) {
    Atom[] atoms;
    List<Atom> atomsList = atomsByColumn.get(columnPath);
    if (atomsList == null) {
      atoms = new Atom[] {};
    } else {
      atoms = (Atom[]) atomsList.toArray();
    }
    return atoms;
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
