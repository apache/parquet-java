package parquet.filter2;

import parquet.filter2.FilterPredicates.And;
import parquet.filter2.FilterPredicates.Column;
import parquet.filter2.FilterPredicates.Eq;
import parquet.filter2.FilterPredicates.Gt;
import parquet.filter2.FilterPredicates.Lt;
import parquet.filter2.FilterPredicates.Not;
import parquet.filter2.FilterPredicates.Or;

/**
 * The Filter API is expressed through these static methods.
 *
 * Example usage:
 * {@code
 *
 *   Column<Integer> foo = column("foo");
 *   Column<Double> bar = column("x.y.bar");
 *
 *   // foo == 10 || bar <= 17.0
 *   FilterPredicate pred = or(eq(foo, 10), ltEq(bar, 17.0));
 *
 * }
 */
public final class Filter {
  private Filter() { }

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
    return new Not(predicate);
  }
}
