package parquet.filter2;

import java.io.Serializable;

import parquet.Preconditions;

public final class FilterPredicates {
  private FilterPredicates() { }

  // The Filter Predicate API is expressed through these static methods

  public static <T> Column<T> column(String columnPath) {
    return new Column<T>(columnPath);
  }

  public static <T> Eq<T> eq(Column<T> column, T value) {
    return new Eq<T>(column, value);
  }

  public static <T> FilterPredicate notEq(Column<T> column, T value) {
    return not(eq(column, value));
  }

  public static <T> Lt<T> lt(Column<T> column, T value) {
    return new Lt<T>(column, value);
  }

  public static <T> FilterPredicate ltEq(Column<T> column, T value) {
    return or(lt(column, value), eq(column, value));
  }

  public static <T> Gt<T> gt(Column<T> column, T value) {
    return new Gt<T>(column, value);
  }

  public static <T> FilterPredicate gtEq(Column<T> column, T value) {
    return or(gt(column, value), eq(column, value));
  }

  public static FilterPredicate and(FilterPredicate left, FilterPredicate right) {
    return new And(left, right);
  }

  public static FilterPredicate or(FilterPredicate left, FilterPredicate right) {
    return new Or(left, right);
  }

  public static FilterPredicate not(FilterPredicate predicate) {
    // TODO: is this the wrong place to do this? There's a lot more
    // TODO: simplification that could be done, but this one is just so easy
    if (predicate instanceof Not) {
      return ((Not) predicate).getPredicate();
    }
    return new Not(predicate);
  }

  // Predicate operator / node data classes

  public static final class Column<T> implements Serializable {
    private final String columnPath;

    private Column(String columnPath) {
      Preconditions.checkNotNull(columnPath, "columnPath");
      this.columnPath = columnPath;
    }

    public String getColumnPath() {
      return columnPath;
    }

    @Override
    public String toString() {
      return "column(" + columnPath + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Column column = (Column) o;
      return columnPath.equals(column.columnPath);
    }

    @Override
    public int hashCode() {
      return columnPath.hashCode();
    }
  }

  // converts "parquet.filter2.FilterPredicates$And" -> "and'
  private static String getClassName(Class<?> c) {
    String name = c.getName();
    // chop off everything up to the first '.'
    name = name.substring(name.lastIndexOf('.') + 1);
    // chop off everything up to the first '$'
    name = name.substring(name.lastIndexOf('$') + 1).toLowerCase();
    return name;
  }

  // base class for Eq, Lt, Gt
  public static abstract class ColumnFilterPredicate<T> implements FilterPredicate, Serializable  {
    private final Column<T> column;
    private final T value;
    private final String toString;

    protected ColumnFilterPredicate(Column<T> column, T value) {
      Preconditions.checkNotNull(column, "column");
      Preconditions.checkNotNull(value, "value");
      this.column = column;
      this.value = value;

      String name = getClassName(getClass());
      this.toString = name + "(" + column.getColumnPath() + ", " + value + ")";
    }

    public Column<T> getColumn() {
      return column;
    }

    public T getValue() {
      return value;
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ColumnFilterPredicate that = (ColumnFilterPredicate) o;

      if (!column.equals(that.column)) return false;
      if (!value.equals(that.value)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = column.hashCode();
      result = 31 * result + value.hashCode();
      result = 31 * result + getClass().hashCode();
      return result;
    }
  }

  public static final class Eq<T> extends ColumnFilterPredicate<T> {

    private Eq(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static final class Lt<T> extends ColumnFilterPredicate<T> {

    private Lt(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static final class Gt<T> extends ColumnFilterPredicate<T> {

    private Gt(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  // base class for And, Or
  private static abstract class BinaryLogicalFilterPredicate implements FilterPredicate, Serializable {
    private final FilterPredicate left;
    private final FilterPredicate right;
    private final String toString;

    protected BinaryLogicalFilterPredicate(FilterPredicate left, FilterPredicate right) {
      Preconditions.checkNotNull(left, "left");
      Preconditions.checkNotNull(right, "right");
      this.left = left;
      this.right = right;
      String name = getClassName(getClass());
      this.toString = name + "(" + left + ", " + right + ")";
    }

    public FilterPredicate getLeft() {
      return left;
    }

    public FilterPredicate getRight() {
      return right;
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BinaryLogicalFilterPredicate that = (BinaryLogicalFilterPredicate) o;

      if (!left.equals(that.left)) return false;
      if (!right.equals(that.right)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = left.hashCode();
      result = 31 * result + right.hashCode();
      result = 31 * result + getClass().hashCode();
      return result;
    }
  }

  public static final class And extends BinaryLogicalFilterPredicate {

    private And(FilterPredicate left, FilterPredicate right) {
      super(left, right);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static final class Or extends BinaryLogicalFilterPredicate {

    private Or(FilterPredicate left, FilterPredicate right) {
      super(left, right);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static class Not implements FilterPredicate, Serializable {
    private final FilterPredicate predicate;
    private final String toString;

    private Not(FilterPredicate predicate) {
      Preconditions.checkNotNull(predicate, "predicate");
      this.predicate = predicate;
      this.toString = "not(" + predicate + ")";
    }

    public FilterPredicate getPredicate() {
      return predicate;
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Not not = (Not) o;
      return predicate.equals(not.predicate);
    }

    @Override
    public int hashCode() {
      return predicate.hashCode() * 31 + getClass().hashCode();
    }
  }
}
