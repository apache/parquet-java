package parquet.filter2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import parquet.Preconditions;
import parquet.column.ColumnDescriptor;
import parquet.filter2.FilterPredicates.And;
import parquet.filter2.FilterPredicates.Column;
import parquet.filter2.FilterPredicates.ColumnFilterPredicate;
import parquet.filter2.FilterPredicates.Eq;
import parquet.filter2.FilterPredicates.Gt;
import parquet.filter2.FilterPredicates.GtEq;
import parquet.filter2.FilterPredicates.Lt;
import parquet.filter2.FilterPredicates.LtEq;
import parquet.filter2.FilterPredicates.Not;
import parquet.filter2.FilterPredicates.NotEq;
import parquet.filter2.FilterPredicates.Or;
import parquet.filter2.FilterPredicates.UserDefined;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;

public class RuntimeTypeChecker implements FilterPredicate.Visitor {
  private final FilterPredicate predicate;
  private final Map<String, ColumnDescriptor> columns = new HashMap<String, ColumnDescriptor>();
  private final Map<String, OriginalType> originalTypes = new HashMap<String, OriginalType>();

  public RuntimeTypeChecker(FilterPredicate predicate, MessageType schema) {
    this.predicate = predicate;

    for (ColumnDescriptor cd : schema.getColumns()) {
      String columnPath = getColumnPath(cd.getPath());

      columns.put(columnPath, cd);

      OriginalType ot = schema.getType(cd.getPath()).getOriginalType();
      if (ot != null) {
        originalTypes.put(columnPath, ot);
      }
    }
  }

  @Override
  public <T> boolean visit(Eq<T> pred) {
    assertTypeValid(pred);
    return false;
  }

  @Override
  public <T> boolean visit(NotEq<T> pred) {
    assertTypeValid(pred);
    return false;
  }

  @Override
  public <T> boolean visit(Lt<T> pred) {
    assertTypeValid(pred);
    return false;
  }

  @Override
  public <T> boolean visit(LtEq<T> pred) {
    assertTypeValid(pred);
    return false;
  }

  @Override
  public <T> boolean visit(Gt<T> pred) {
    assertTypeValid(pred);
    return false;
  }

  @Override
  public <T> boolean visit(GtEq<T> pred) {
    assertTypeValid(pred);
    return false;
  }

  @Override
  public boolean visit(And and) {
    return false;
  }

  @Override
  public boolean visit(Or or) {
    return false;
  }

  @Override
  public boolean visit(Not not) {
    return false;
  }

  @Override
  public <T, U> boolean visit(UserDefined<T, U> udp) {
    assertTypeValid(udp.getColumn());
    return false;
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

  private static String getColumnPath(String[] path) {
    StringBuilder sb = new StringBuilder();
    Iterator<String> iter = Arrays.asList(path).iterator();
    while(iter.hasNext()) {
      sb.append(iter.next());

      if (iter.hasNext()) {
        sb.append('.');
      }
    }
    return sb.toString();
  }

}
