package parquet.filter;

import parquet.Preconditions;
import parquet.column.ColumnReader;

/**
 * ColumnPredicates class provides checks for column values. Factory methods
 * are provided for standard predicates which wrap the job of getting the
 * correct value from the column.
 */
public class ColumnPredicates {

  public static interface Predicate {
    boolean apply(ColumnReader input);
  }

  public static Predicate equalTo(final String target) {
    Preconditions.checkNotNull(target,"target");
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
        return target.equals(input.getBinary().toStringUsingUTF8());
      }
    };
  }

  public static Predicate equalTo(final int target) {
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
        return input.getInteger() == target;
      }
    };
  }

  public static Predicate equalTo(final long target) {
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
        return input.getLong() == target;
      }
    };
  }

  public static Predicate equalTo(final float target) {
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
        return input.getFloat() == target;
      }
    };
  }

  public static Predicate equalTo(final double target) {
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
        return input.getDouble() == target;
      }
    };
  }

  public static Predicate equalTo(final boolean target) {
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
        return input.getBoolean() == target;
      }
    };
  }

  public static <E extends Enum> Predicate equalTo(final E target) {
    Preconditions.checkNotNull(target,"target");
    final String targetAsString = target.name();
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
        return targetAsString.equals(input.getBinary().toStringUsingUTF8());
      }
    };
  }
}
