package parquet.filter2;

import java.util.HashMap;
import java.util.Map;

import parquet.Preconditions;
import parquet.column.ColumnDescriptor;
import parquet.filter2.FilterPredicates.And;
import parquet.filter2.FilterPredicates.Column;
import parquet.filter2.FilterPredicates.ColumnFilterPredicate;
import parquet.filter2.FilterPredicates.Eq;
import parquet.filter2.FilterPredicates.Gt;
import parquet.filter2.FilterPredicates.GtEq;
import parquet.filter2.FilterPredicates.LogicalNotUserDefined;
import parquet.filter2.FilterPredicates.Lt;
import parquet.filter2.FilterPredicates.LtEq;
import parquet.filter2.FilterPredicates.Not;
import parquet.filter2.FilterPredicates.NotEq;
import parquet.filter2.FilterPredicates.Or;
import parquet.filter2.FilterPredicates.UserDefined;
import parquet.schema.ColumnPathUtil;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;

public class RuntimeFilterValidator implements FilterPredicate.Visitor<Void> {

  public static void validate(FilterPredicate predicate, MessageType schema) {
    FilterPredicate.Visitor validator = new RuntimeFilterValidator(schema);
    predicate.accept(validator);
  }

  private final Map<String, ColumnDescriptor> columns = new HashMap<String, ColumnDescriptor>();
  private final Map<String, OriginalType> originalTypes = new HashMap<String, OriginalType>();

  public RuntimeFilterValidator(MessageType schema) {

    for (ColumnDescriptor cd : schema.getColumns()) {
      String columnPath = ColumnPathUtil.toDotSeparatedString(cd.getPath());

      columns.put(columnPath, cd);

      OriginalType ot = schema.getType(cd.getPath()).getOriginalType();
      if (ot != null) {
        originalTypes.put(columnPath, ot);
      }
    }
  }

  @Override
  public <T> Void visit(Eq<T> pred) {
    assertTypeValid(pred);
    return null;
  }

  @Override
  public <T> Void visit(NotEq<T> pred) {
    assertTypeValid(pred);
    return null;
  }

  @Override
  public <T> Void visit(Lt<T> pred) {
    assertTypeValid(pred);
    return null;
  }

  @Override
  public <T> Void visit(LtEq<T> pred) {
    assertTypeValid(pred);
    return null;
  }

  @Override
  public <T> Void visit(Gt<T> pred) {
    assertTypeValid(pred);
    return null;
  }

  @Override
  public <T> Void visit(GtEq<T> pred) {
    assertTypeValid(pred);
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
  public <T, U> Void visit(UserDefined<T, U> udp) {
    assertTypeValid(udp.getColumn());
    return null;
  }

  @Override
  public <T, U> Void visit(LogicalNotUserDefined<T, U> udp) {
    return udp.getUserDefined().accept(this);
  }

  private <T> void assertTypeValid(ColumnFilterPredicate<T> pred) {
    assertTypeValid(pred.getColumn());
  }

  private <T> void assertTypeValid(Column<T> column) {
    String path = column.getColumnPath();
    ValidTypeMap.assertTypeValid(
        path,
        column.getColumnType(),
        getColumnDescriptor(path).getType(),
        originalTypes.get(path));
  }

  private ColumnDescriptor getColumnDescriptor(String columnPath) {
    ColumnDescriptor cd = columns.get(columnPath);
    Preconditions.checkArgument(cd != null, "Column " + columnPath + " was not found in schema!");
    return cd;
  }
}
