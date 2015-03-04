/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.filter2.predicate;

import java.io.Serializable;

import parquet.common.schema.ColumnPath;
import parquet.io.api.Binary;

import static parquet.Preconditions.checkNotNull;

/**
 * These are the operators in a filter predicate expression tree.
 * They are constructed by using the methods in {@link FilterApi}
 */
public final class Operators {
  private Operators() { }

  public static abstract class Column<T extends Comparable<T>> implements Serializable {
    private final ColumnPath columnPath;
    private final Class<T> columnType;

    protected Column(ColumnPath columnPath, Class<T> columnType) {
      checkNotNull(columnPath, "columnPath");
      checkNotNull(columnType, "columnType");
      this.columnPath = columnPath;
      this.columnType = columnType;
    }

    public Class<T> getColumnType() {
      return columnType;
    }

    public ColumnPath getColumnPath() {
      return columnPath;
    }

    @Override
    public String toString() {
      return "column(" + columnPath.toDotString() + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Column column = (Column) o;

      if (!columnType.equals(column.columnType)) return false;
      if (!columnPath.equals(column.columnPath)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = columnPath.hashCode();
      result = 31 * result + columnType.hashCode();
      return result;
    }
  }

  public static interface SupportsEqNotEq { } // marker for columns that can be used with eq() and notEq()
  public static interface SupportsLtGt extends SupportsEqNotEq { } // marker for columns that can be used with lt(), ltEq(), gt(), gtEq()

  public static final class IntColumn extends Column<Integer> implements SupportsLtGt {
    IntColumn(ColumnPath columnPath) {
      super(columnPath, Integer.class);
    }
  }

  public static final class LongColumn extends Column<Long> implements SupportsLtGt {
    LongColumn(ColumnPath columnPath) {
      super(columnPath, Long.class);
    }
  }

  public static final class DoubleColumn extends Column<Double> implements SupportsLtGt {
    DoubleColumn(ColumnPath columnPath) {
      super(columnPath, Double.class);
    }
  }

  public static final class FloatColumn extends Column<Float> implements SupportsLtGt {
    FloatColumn(ColumnPath columnPath) {
      super(columnPath, Float.class);
    }
  }

  public static final class BooleanColumn extends Column<Boolean> implements SupportsEqNotEq {
    BooleanColumn(ColumnPath columnPath) {
      super(columnPath, Boolean.class);
    }
  }

  public static final class BinaryColumn extends Column<Binary> implements SupportsLtGt {
    BinaryColumn(ColumnPath columnPath) {
      super(columnPath, Binary.class);
    }
  }

  // base class for Eq, NotEq, Lt, Gt, LtEq, GtEq
  static abstract class ColumnFilterPredicate<T extends Comparable<T>> implements FilterPredicate, Serializable  {
    private final Column<T> column;
    private final T value;
    private final String toString;

    protected ColumnFilterPredicate(Column<T> column, T value) {
      this.column = checkNotNull(column, "column");

      // Eq and NotEq allow value to be null, Lt, Gt, LtEq, GtEq however do not, so they guard against
      // null in their own constructors.
      this.value = value;

      String name = getClass().getSimpleName().toLowerCase();
      this.toString = name + "(" + column.getColumnPath().toDotString() + ", " + value + ")";
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
      if (value != null ? !value.equals(that.value) : that.value != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = column.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + getClass().hashCode();
      return result;
    }
  }

  public static final class Eq<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

    // value can be null
    Eq(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }

  }

  public static final class NotEq<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

    // value can be null
    NotEq(Column<T> column, T value) {
      super(column, value);
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }


  public static final class Lt<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

    // value cannot be null
    Lt(Column<T> column, T value) {
      super(column, checkNotNull(value, "value"));
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  public static final class LtEq<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

    // value cannot be null
    LtEq(Column<T> column, T value) {
      super(column, checkNotNull(value, "value"));
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }


  public static final class Gt<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

    // value cannot be null
    Gt(Column<T> column, T value) {
      super(column, checkNotNull(value, "value"));
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  public static final class GtEq<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

    // value cannot be null
    GtEq(Column<T> column, T value) {
      super(column, checkNotNull(value, "value"));
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  // base class for And, Or
  private static abstract class BinaryLogicalFilterPredicate implements FilterPredicate, Serializable {
    private final FilterPredicate left;
    private final FilterPredicate right;
    private final String toString;

    protected BinaryLogicalFilterPredicate(FilterPredicate left, FilterPredicate right) {
      this.left = checkNotNull(left, "left");
      this.right = checkNotNull(right, "right");
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
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  public static final class Or extends BinaryLogicalFilterPredicate {

    Or(FilterPredicate left, FilterPredicate right) {
      super(left, right);
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  public static class Not implements FilterPredicate, Serializable {
    private final FilterPredicate predicate;
    private final String toString;

    Not(FilterPredicate predicate) {
      this.predicate = checkNotNull(predicate, "predicate");
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
    public <R> R accept(Visitor<R> visitor) {
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

  public static abstract class UserDefined<T extends Comparable<T>, U extends UserDefinedPredicate<T>> implements FilterPredicate, Serializable {
    protected final Column<T> column;

    UserDefined(Column<T> column) {
      this.column = checkNotNull(column, "column");
    }

    public Column<T> getColumn() {
      return column;
    }

    public abstract U getUserDefinedPredicate();

    @Override
    public <R> R accept(Visitor<R> visitor) {
      return visitor.visit(this);
    }
  }
    
  public static final class UserDefinedByClass<T extends Comparable<T>, U extends UserDefinedPredicate<T>> extends UserDefined<T, U> {
    private final Class<U> udpClass;
    private final String toString;
    private static final String INSTANTIATION_ERROR_MESSAGE =
        "Could not instantiate custom filter: %s. User defined predicates must be static classes with a default constructor.";

    UserDefinedByClass(Column<T> column, Class<U> udpClass) {
      super(column);
      this.udpClass = checkNotNull(udpClass, "udpClass");
      String name = getClass().getSimpleName().toLowerCase();
      this.toString = name + "(" + column.getColumnPath().toDotString() + ", " + udpClass.getName() + ")";

      // defensively try to instantiate the class early to make sure that it's possible
      getUserDefinedPredicate();
    }

    public Class<U> getUserDefinedPredicateClass() {
      return udpClass;
    }

    @Override
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
    public String toString() {
      return toString;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      UserDefinedByClass that = (UserDefinedByClass) o;

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
  
  public static final class UserDefinedByInstance<T extends Comparable<T>, U extends UserDefinedPredicate<T> & Serializable> extends UserDefined<T, U> {
    private final String toString;
    private final U udpInstance;

    UserDefinedByInstance(Column<T> column, U udpInstance) {
      super(column);
      this.udpInstance = checkNotNull(udpInstance, "udpInstance");
      String name = getClass().getSimpleName().toLowerCase();
      this.toString = name + "(" + column.getColumnPath().toDotString() + ", " + udpInstance + ")";
    }

    @Override
    public U getUserDefinedPredicate() {
      return udpInstance;
    }

    @Override
    public String toString() {
      return toString;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      UserDefinedByInstance that = (UserDefinedByInstance) o;

      if (!column.equals(that.column)) return false;
      if (!udpInstance.equals(that.udpInstance)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = column.hashCode();
      result = 31 * result + udpInstance.hashCode();
      result = result * 31 + getClass().hashCode();
      return result;
    }
  }

  // Represents the inverse of a UserDefined. It is equivalent to not(userDefined), without the use
  // of the not() operator
  public static final class LogicalNotUserDefined <T extends Comparable<T>, U extends UserDefinedPredicate<T>> implements FilterPredicate, Serializable {
    private final UserDefined<T, U> udp;
    private final String toString;

    LogicalNotUserDefined(UserDefined<T, U> userDefined) {
      this.udp = checkNotNull(userDefined, "userDefined");
      this.toString = "inverted(" + udp + ")";
    }

    public UserDefined<T, U> getUserDefined() {
      return udp;
    }

    @Override
    public <R> R accept(Visitor<R> visitor) {
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

      LogicalNotUserDefined that = (LogicalNotUserDefined) o;

      if (!udp.equals(that.udp)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = udp.hashCode();
      result = result * 31 + getClass().hashCode();
      return result;
    }
  }

}
