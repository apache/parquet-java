/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

  public static interface PredicateFunction <T> {
    boolean functionToApply(T input);
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

  public static Predicate applyFunctionToString(final PredicateFunction<String> fn) {
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
	  return fn.functionToApply(input.getBinary().toStringUsingUTF8());
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

  public static Predicate applyFunctionToInteger(final PredicateFunction<Integer> fn) {
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
	return fn.functionToApply(new Integer (input.getInteger()));
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

  public static Predicate applyFunctionToLong(final PredicateFunction<Long> fn) {
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
	return fn.functionToApply(new Long (input.getLong()));
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

  public static Predicate applyFunctionToFloat(final PredicateFunction<Float> fn) {
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
	return fn.functionToApply(new Float (input.getFloat()));
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

  public static Predicate applyFunctionToDouble(final PredicateFunction<Double> fn) {
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
	return fn.functionToApply(new Double (input.getDouble()));
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

  public static Predicate applyFunctionToBoolean (final PredicateFunction<Boolean> fn) {
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
	return fn.functionToApply(new Boolean (input.getBoolean()));
      }
    };
  }

  public static <E extends Enum> Predicate equalTo(final E target) {
    final String targetAsString = target.name();
    return new Predicate() {
      @Override
      public boolean apply(ColumnReader input) {
        return targetAsString.equals(input.getBinary().toStringUsingUTF8());
      }
    };
  }
}
