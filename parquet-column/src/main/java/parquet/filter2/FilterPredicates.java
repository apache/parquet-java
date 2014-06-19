package parquet.filter2;

import java.io.Serializable;

import parquet.Preconditions;
import parquet.filter2.UserDefinedPredicates.BinaryUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.DoubleUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.FloatUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.IntUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.LongUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.StringUserDefinedPredicate;

/**
 * These are the nodes / tokens in a a filter predicate expression.
 * They are constructed by using the methods in {@link Filter}
 */
public final class FilterPredicates {
  private FilterPredicates() { }

  public static final class Column<T> implements Serializable {
    private final String columnPath;

    Column(String columnPath) {
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


  // base class for Eq, Lt, Gt
  private static abstract class ColumnFilterPredicate<T> implements FilterPredicate, Serializable  {
    private final Column<T> column;
    private final T value;
    private final String toString;

    protected ColumnFilterPredicate(Column<T> column, T value) {
      Preconditions.checkNotNull(column, "column");
      Preconditions.checkNotNull(value, "value");
      this.column = column;
      this.value = value;

      String name = getClass().getSimpleName().toLowerCase();
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

    Eq(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static final class NotEq<T> extends ColumnFilterPredicate<T> {

    NotEq(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }


  public static final class Lt<T> extends ColumnFilterPredicate<T> {

    Lt(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static final class LtEq<T> extends ColumnFilterPredicate<T> {

    LtEq(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }


  public static final class Gt<T> extends ColumnFilterPredicate<T> {

    Gt(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static final class GtEq<T> extends ColumnFilterPredicate<T> {

    GtEq(Column<T> column, T value) {
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
      String name = getClass().getSimpleName().toLowerCase();
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

    And(FilterPredicate left, FilterPredicate right) {
      super(left, right);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static final class Or extends BinaryLogicalFilterPredicate {

    Or(FilterPredicate left, FilterPredicate right) {
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

    Not(FilterPredicate predicate) {
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

  public static abstract class UserDefined<T, U> implements FilterPredicate, Serializable {
    private final Column<T> column;
    private final Class<U> udpClass;
    private final String toString;
    private static final String INSTANTIATION_ERROR_MESSAGE =
        "Could not instantiate custom filter: %s. User defined predicates must be static classes with a default constructor.";

    UserDefined(Column<T> column, Class<U> udpClass) {
      Preconditions.checkNotNull(column, "column");
      Preconditions.checkNotNull(udpClass, "userDefinedPredicate");
      this.column = column;
      this.udpClass = udpClass;
      String name = getClass().getSimpleName().toLowerCase();
      this.toString = name + "(" + column.getColumnPath() + ", " + udpClass.getName() + ")";

      getUserDefinedPredicate();
    }

    public Class<U> getUserDefinedPredicateClass() {
      return udpClass;
    }

    public U getUserDefinedPredicate() {
      try {
        return udpClass.newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException(String.format(INSTANTIATION_ERROR_MESSAGE, udpClass), e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(String.format(INSTANTIATION_ERROR_MESSAGE, udpClass), e);
      }
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      UserDefined that = (UserDefined) o;

      if (!column.equals(that.column)) return false;
      if (!udpClass.equals(that.udpClass)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = column.hashCode();
      result = 31 * result + udpClass.hashCode();
      result = result * 31 + getClass().hashCode();
      return result;
    }
  }

  public static class IntUserDefined<T extends IntUserDefinedPredicate> extends UserDefined<Integer, T> {
    IntUserDefined(Column<Integer> column, Class<T> udpClass) {
      super(column, udpClass);
    }
  }

  public static class LongUserDefined<T extends LongUserDefinedPredicate> extends UserDefined<Long, T> {
    LongUserDefined(Column<Long> column, Class<T> udpClass) {
      super(column, udpClass);
    }
  }

  public static class FloatUserDefined<T extends FloatUserDefinedPredicate> extends UserDefined<Float, T> {
    FloatUserDefined(Column<Float> column, Class<T> udpClass) {
      super(column, udpClass);
    }
  }

  public static class DoubleUserDefined<T extends DoubleUserDefinedPredicate> extends UserDefined<Double, T> {
    DoubleUserDefined(Column<Double> column, Class<T> udpClass) {
      super(column, udpClass);
    }
  }

  public static class BinaryUserDefined<T extends BinaryUserDefinedPredicate> extends UserDefined<byte[], T> {
    BinaryUserDefined(Column<byte[]> column, Class<T> udpClass) {
      super(column, udpClass);
    }
  }

  public static class StringUserDefined<T extends StringUserDefinedPredicate> extends UserDefined<String, T> {
    StringUserDefined(Column<String> column, Class<T> udpClass) {
      super(column, udpClass);
    }
  }

}
