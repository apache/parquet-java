package parquet.filter2;

import parquet.ColumnPath;
import parquet.filter2.FilterPredicateOperators.And;
import parquet.filter2.FilterPredicateOperators.BinaryColumn;
import parquet.filter2.FilterPredicateOperators.BooleanColumn;
import parquet.filter2.FilterPredicateOperators.Column;
import parquet.filter2.FilterPredicateOperators.DoubleColumn;
import parquet.filter2.FilterPredicateOperators.Eq;
import parquet.filter2.FilterPredicateOperators.FloatColumn;
import parquet.filter2.FilterPredicateOperators.Gt;
import parquet.filter2.FilterPredicateOperators.GtEq;
import parquet.filter2.FilterPredicateOperators.IntColumn;
import parquet.filter2.FilterPredicateOperators.LongColumn;
import parquet.filter2.FilterPredicateOperators.Lt;
import parquet.filter2.FilterPredicateOperators.LtEq;
import parquet.filter2.FilterPredicateOperators.Not;
import parquet.filter2.FilterPredicateOperators.NotEq;
import parquet.filter2.FilterPredicateOperators.Or;
import parquet.filter2.FilterPredicateOperators.SupportsEqNotEq;
import parquet.filter2.FilterPredicateOperators.SupportsLtGt;
import parquet.filter2.FilterPredicateOperators.UserDefined;
import parquet.io.api.Binary;

/**
 * The Filter API is expressed through these static methods.
 *
 * Example usage:
 * {@code
 *
 *   IntColumn foo = intColumn("foo");
 *   DoubleColumn bar = doubleColumn("x.y.bar");
 *
 *   // foo == 10 || bar <= 17.0
 *   FilterPredicate pred = or(eq(foo, 10), ltEq(bar, 17.0));
 *
 * }
 */
// TODO(alexlevenson): Support repeated columns
// TODO(alexlevenson): Add support for more column types that aren't coupled with parquet types, eg Column<String>
public final class Filter {
  private Filter() { }

  public static IntColumn intColumn(String columnPath) {
    return new IntColumn(ColumnPath.get(columnPath.split("\\.")));
  }

  public static LongColumn longColumn(String columnPath) {
    return new LongColumn(ColumnPath.get(columnPath.split("\\.")));
  }

  public static FloatColumn floatColumn(String columnPath) {
    return new FloatColumn(ColumnPath.get(columnPath.split("\\.")));
  }

  public static DoubleColumn doubleColumn(String columnPath) {
    return new DoubleColumn(ColumnPath.get(columnPath.split("\\.")));
  }

  public static BooleanColumn booleanColumn(String columnPath) {
    return new BooleanColumn(ColumnPath.get(columnPath.split("\\.")));
  }

  public static BinaryColumn binaryColumn(String columnPath) {
    return new BinaryColumn(ColumnPath.get(columnPath.split("\\.")));
  }

  /**
   * Keeps records if their value is equal to the provided value.
   * Nulls are treated the same way the java programming language does.
   * For example:
   *   eq(column, null) will keep all records whose value is null.
   *   eq(column, 7) will keep all records whose value is 7, and will skip records whose value is null
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsEqNotEq> Eq<T> eq(C column, T value) {
    return new Eq<T>(column, value);
  }

  /**
   * Keeps records if their value is not equal to the provided value.
   * Nulls are treated the same way the java programming language does.
   * For example:
   *   notEq(column, null) will keep all records whose value is not null.
   *   notEq(column, 7) will keep all records whose value is not 7, including records whose value is null.
   *
   *   NOTE: this is different from how some query languages handle null. For example, SQL and pig will drop
   *   nulls when you filter by not equal to 7. To achieve similar behavior in this api, do:
   *   and(notEq(column, 7), notEq(column, null))
   *
   *   NOTE: be sure to read the {@link #lt}, {@link #ltEq}, {@link #gt}, {@link #gtEq} operator's docs
   *         for how they handle nulls
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsEqNotEq> NotEq<T> notEq(C column, T value) {
    return new NotEq<T>(column, value);
  }

  /**
   * Keeps records if their value is less than (but not equal to) the provided value.
   * The provided value cannot be null, as less than null has no meaning.
   * Records with null values will be skipped.
   * For example:
   *   lt(column, 7) will keep all records whose value is less than (but not equal to) 7, and not null.
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsLtGt> Lt<T> lt(C column, T value) {
    return new Lt<T>(column, value);
  }

  /**
   * Keeps records if their value is less than or equal to the provided value.
   * The provided value cannot be null, as less than null has no meaning.
   * Records with null values will be skipped.
   * For example:
   *   ltEq(column, 7) will keep all records whose value is less than or equal to 7, and not null.
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsLtGt> LtEq<T> ltEq(C column, T value) {
    return new LtEq<T>(column, value);
  }

  /**
   * Keeps records if their value is greater than (but not equal to) the provided value.
   * The provided value cannot be null, as less than null has no meaning.
   * Records with null values will be skipped.
   * For example:
   *   gt(column, 7) will keep all records whose value is greater than (but not equal to) 7, and not null.
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsLtGt> Gt<T> gt(C column, T value) {
    return new Gt<T>(column, value);
  }

  /**
   * Keeps records if their value is greater than or equal to the provided value.
   * The provided value cannot be null, as less than null has no meaning.
   * Records with null values will be skipped.
   * For example:
   *   gtEq(column, 7) will keep all records whose value is greater than or equal to 7, and not null.
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsLtGt> GtEq<T> gtEq(C column, T value) {
    return new GtEq<T>(column, value);
  }

  /**
   * Keeps records that pass the provided {@link parquet.filter2.UserDefinedPredicate}
   */
  public static <T extends Comparable<T>, U extends UserDefinedPredicate<T>>
    UserDefined<T, U> userDefined(Column<T> column, Class<U> clazz) {
    return new UserDefined<T, U>(column, clazz);
  }

  /**
   * Constructs the logical and of two predicates. Records will be kept if both the left and right predicate agree
   * that the record should be kept.
   */
  public static FilterPredicate and(FilterPredicate left, FilterPredicate right) {
    return new And(left, right);
  }

  /**
   * Constructs the logical or of two predicates. Records will be kept if either the left or right predicate
   * is satisfied (or both).
   */
  public static FilterPredicate or(FilterPredicate left, FilterPredicate right) {
    return new Or(left, right);
  }

  /**
   * Constructs the logical not (or inverse) of a predicate.
   * Records will be kept if the provided predicate is not satisfied.
   */
  public static FilterPredicate not(FilterPredicate predicate) {
    return new Not(predicate);
  }

}
