package parquet.filter2;

import java.util.HashMap;
import java.util.Map;

import parquet.Preconditions;
import parquet.column.ColumnDescriptor;
import parquet.filter2.FilterPredicateOperators.And;
import parquet.filter2.FilterPredicateOperators.Column;
import parquet.filter2.FilterPredicateOperators.ColumnFilterPredicate;
import parquet.filter2.FilterPredicateOperators.Eq;
import parquet.filter2.FilterPredicateOperators.Gt;
import parquet.filter2.FilterPredicateOperators.GtEq;
import parquet.filter2.FilterPredicateOperators.LogicalNotUserDefined;
import parquet.filter2.FilterPredicateOperators.Lt;
import parquet.filter2.FilterPredicateOperators.LtEq;
import parquet.filter2.FilterPredicateOperators.Not;
import parquet.filter2.FilterPredicateOperators.NotEq;
import parquet.filter2.FilterPredicateOperators.Or;
import parquet.filter2.FilterPredicateOperators.UserDefined;
import parquet.filter2.UserDefinedPredicates.UserDefinedPredicate;
import parquet.schema.ColumnPathUtil;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;

/**
 * Inspects the column types found in the provided FilterPredicate and compares them
 * to the actual schema found in the parquet file. If the provided predicate's types are
 * not consistent with the file schema, and IllegalArgumentException is thrown.
 *
 * Ideally, all this would be checked at compile time, and this class wouldn't be needed.
 * If we can come up with a way to do that, we should.
 *
 * TODO: detect if a column is optional or required and validate that eq(null)
 * TODO: is not called on optional fields
 */
public class FilterPredicateTypeValidator implements FilterPredicate.Visitor<Void> {

  public static void validate(FilterPredicate predicate, MessageType schema) {
    predicate.accept(new FilterPredicateTypeValidator(schema));
  }

  // A map of column name to the type the user supplied for this column.
  // Used to validate that the user did not provide different types for the same
  // column.
  private final Map<String, Class<?>> columnTypesEncountered = new HashMap<String, Class<?>>();

  // the columns (keyed by path) according to the file's schema. This is the source of truth, and
  // we are validating that what the user provided agrees with these.
  private final Map<String, ColumnDescriptor> columnsAccordingToSchema = new HashMap<String, ColumnDescriptor>();

  // the original type of a column, keyed by path
  private final Map<String, OriginalType> originalTypes = new HashMap<String, OriginalType>();

  public FilterPredicateTypeValidator(MessageType schema) {

    for (ColumnDescriptor cd : schema.getColumns()) {
      String columnPath = ColumnPathUtil.toDotSeparatedString(cd.getPath());
      columnsAccordingToSchema.put(columnPath, cd);

      OriginalType ot = schema.getType(cd.getPath()).getOriginalType();
      if (ot != null) {
        originalTypes.put(columnPath, ot);
      }
    }
  }

  @Override
  public <T> Void visit(Eq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T> Void visit(NotEq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T> Void visit(Lt<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T> Void visit(LtEq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T> Void visit(Gt<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T> Void visit(GtEq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public Void visit(And and) {
    and.getLeft().accept(this);
    and.getRight().accept(this);
    return null;
  }

  @Override
  public Void visit(Or or) {
    or.getLeft().accept(this);
    or.getRight().accept(this);
    return null;
  }

  @Override
  public Void visit(Not not) {
    not.getPredicate().accept(this);
    return null;
  }

  @Override
  public <T, U extends UserDefinedPredicate<T>> Void visit(UserDefined<T, U> udp) {
    validateColumn(udp.getColumn());
    return null;
  }

  @Override
  public <T, U extends UserDefinedPredicate<T>> Void visit(LogicalNotUserDefined<T, U> udp) {
    return udp.getUserDefined().accept(this);
  }

  private <T> void validateColumnFilterPredicate(ColumnFilterPredicate<T> pred) {
    validateColumn(pred.getColumn());
  }

  private <T> void validateColumn(Column<T> column) {
    String path = column.getColumnPath();

    Class<?> alreadySeen = columnTypesEncountered.get(path);
    if (alreadySeen != null && !alreadySeen.equals(column.getColumnType())) {
      throw new IllegalArgumentException("Column: "
          + path
          + " was provided with different types in the same predicate."
          + " Found both: (" + alreadySeen + ", " + column.getColumnType() + ")");
    }
    columnTypesEncountered.put(path, column.getColumnType());

    ValidTypeMap.assertTypeValid(column, getColumnDescriptor(path).getType(), originalTypes.get(path));
  }

  private ColumnDescriptor getColumnDescriptor(String columnPath) {
    ColumnDescriptor cd = columnsAccordingToSchema.get(columnPath);
    Preconditions.checkArgument(cd != null, "Column " + columnPath + " was not found in schema!");
    return cd;
  }
}
