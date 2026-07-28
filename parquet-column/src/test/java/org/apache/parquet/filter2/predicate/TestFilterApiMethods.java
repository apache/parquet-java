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
package org.apache.parquet.filter2.predicate;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.contains;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.filter2.predicate.Operators.NotEq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.filter2.predicate.Operators.Not;
import org.apache.parquet.filter2.predicate.Operators.Or;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;
import org.apache.parquet.filter2.predicate.Operators.UserDefinedByClass;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

public class TestFilterApiMethods {

  private static final IntColumn intColumn = intColumn("a.b.c");
  private static final LongColumn longColumn = longColumn("a.b.l");
  private static final DoubleColumn doubleColumn = doubleColumn("x.y.z");
  private static final BinaryColumn binColumn = binaryColumn("a.string.column");

  private static final FilterPredicate predicate =
      and(not(or(eq(intColumn, 7), notEq(intColumn, 17))), gt(doubleColumn, 100.0));

  @Test
  public void testFilterPredicateCreation() {
    FilterPredicate outerAnd = predicate;

    assertThat(outerAnd).isInstanceOf(And.class);

    FilterPredicate not = ((And) outerAnd).getLeft();
    FilterPredicate gt = ((And) outerAnd).getRight();
    assertThat(not).isInstanceOf(Not.class);

    FilterPredicate or = ((Not) not).getPredicate();
    assertThat(or).isInstanceOf(Or.class);

    FilterPredicate leftEq = ((Or) or).getLeft();
    FilterPredicate rightNotEq = ((Or) or).getRight();
    assertThat(leftEq).isInstanceOf(Eq.class);
    assertThat(rightNotEq).isInstanceOf(NotEq.class);
    assertThat(((Eq) leftEq).getValue()).isEqualTo(7);
    assertThat(((NotEq) rightNotEq).getValue()).isEqualTo(17);
    assertThat(((Eq) leftEq).getColumn().getColumnPath()).isEqualTo(ColumnPath.get("a", "b", "c"));
    assertThat(((NotEq) rightNotEq).getColumn().getColumnPath()).isEqualTo(ColumnPath.get("a", "b", "c"));

    assertThat(gt).isInstanceOf(Gt.class);
    assertThat(((Gt) gt).getValue()).isEqualTo(100.0);
    assertThat(((Gt) gt).getColumn().getColumnPath()).isEqualTo(ColumnPath.get("x", "y", "z"));
  }

  @Test
  public void testContainsCreation() {
    assertThatThrownBy(() -> contains(eq(binColumn, null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Contains predicate does not support null element value(s)");

    // Assert that a single Contains predicate referencing multiple columns throws an error
    assertThatThrownBy(() -> contains(eq(binaryColumn("a.b.c"), Binary.fromString("foo")))
            .and(contains(eq(binaryColumn("b.c.d"), Binary.fromString("bar"))))
            .and(contains(eq(binaryColumn("b.c.d"), Binary.fromString("bar")))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Composed Contains predicates must reference the same column name; found [a.b.c, b.c.d]");

    // Assert that a Contains predicate referencing multiple columns is allowed when composed with and() or or()
    final FilterPredicate rewritten = ContainsRewriter.rewrite(or(
        contains(eq(binaryColumn("a.b.c"), Binary.fromString("foo"))),
        and(
            contains(eq(binaryColumn("b.c.d"), Binary.fromString("bar"))),
            contains(eq(binaryColumn("b.c.d"), Binary.fromString("baz"))))));
    assertThat(rewritten).isInstanceOf(Or.class);

    // Assert that the predicates for column b.c.d have been combined into a single Contains predicate,
    // while the predicate for column a.b.c is separate
    final Or or = (Or) rewritten;
    assertThat(((Operators.Contains) or.getLeft()).getColumn()).isEqualTo(binaryColumn("a.b.c"));
    assertThat(((Operators.Contains) or.getRight()).getColumn()).isEqualTo(binaryColumn("b.c.d"));
  }

  @Test
  public void testToString() {
    FilterPredicate pred = or(predicate, notEq(binColumn, Binary.fromString("foobarbaz")));
    assertThat(pred)
        .asString()
        .isEqualTo("or(and(not(or(eq(a.b.c, 7), noteq(a.b.c, 17))), gt(x.y.z, 100.0)), "
            + "noteq(a.string.column, Binary{\"foobarbaz\"}))");

    pred = ContainsRewriter.rewrite(or(
        contains(eq(binColumn, Binary.fromString("foo"))),
        and(
            contains(eq(binColumn, Binary.fromString("bar"))),
            not(contains(eq(binColumn, Binary.fromString("baz")))))));
    assertThat(pred)
        .asString()
        .isEqualTo(
            "or(contains(eq(a.string.column, Binary{\"foo\"})), and(contains(eq(a.string.column, Binary{\"bar\"})), not(contains(eq(a.string.column, Binary{\"baz\"})))))");
  }

  @Test
  public void testUdp() {
    FilterPredicate predicate = or(eq(doubleColumn, 12.0), userDefined(intColumn, DummyUdp.class));
    assertThat(predicate).isInstanceOf(Or.class);
    FilterPredicate ud = ((Or) predicate).getRight();
    assertThat(ud).isInstanceOf(UserDefinedByClass.class);
    assertThat(((UserDefinedByClass) ud).getUserDefinedPredicateClass()).isEqualTo(DummyUdp.class);
    assertThat(((UserDefined) ud).getUserDefinedPredicate()).isInstanceOf(DummyUdp.class);
  }

  @Test
  public void testSerializable() throws Exception {
    BinaryColumn binary = binaryColumn("foo");
    FilterPredicate p = and(
        or(and(userDefined(intColumn, DummyUdp.class), predicate), eq(binary, Binary.fromString("hi"))),
        userDefined(longColumn, new IsMultipleOf(7)));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(p);
    oos.close();

    ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    FilterPredicate read = (FilterPredicate) is.readObject();
    assertThat(read).isEqualTo(p);
  }

  public static class IsMultipleOf extends UserDefinedPredicate<Long> implements Serializable {

    private long of;

    public IsMultipleOf(long of) {
      this.of = of;
    }

    @Override
    public boolean keep(Long value) {
      if (value == null) {
        return false;
      }
      return value % of == 0;
    }

    @Override
    public boolean canDrop(Statistics<Long> statistics) {
      return false;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Long> statistics) {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IsMultipleOf that = (IsMultipleOf) o;
      return this.of == that.of;
    }

    @Override
    public int hashCode() {
      return new Long(of).hashCode();
    }

    @Override
    public String toString() {
      return "IsMultipleOf(" + of + ")";
    }
  }
}
