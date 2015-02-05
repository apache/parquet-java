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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.junit.Test;

import parquet.common.schema.ColumnPath;
import parquet.filter2.predicate.Operators.And;
import parquet.filter2.predicate.Operators.BinaryColumn;
import parquet.filter2.predicate.Operators.DoubleColumn;
import parquet.filter2.predicate.Operators.Eq;
import parquet.filter2.predicate.Operators.Gt;
import parquet.filter2.predicate.Operators.IntColumn;
import parquet.filter2.predicate.Operators.LongColumn;
import parquet.filter2.predicate.Operators.Not;
import parquet.filter2.predicate.Operators.Or;
import parquet.filter2.predicate.Operators.UserDefined;
import parquet.filter2.predicate.Operators.UserDefinedByClass;
import parquet.io.api.Binary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static parquet.filter2.predicate.FilterApi.and;
import static parquet.filter2.predicate.FilterApi.binaryColumn;
import static parquet.filter2.predicate.FilterApi.doubleColumn;
import static parquet.filter2.predicate.FilterApi.eq;
import static parquet.filter2.predicate.FilterApi.gt;
import static parquet.filter2.predicate.FilterApi.intColumn;
import static parquet.filter2.predicate.FilterApi.longColumn;
import static parquet.filter2.predicate.FilterApi.not;
import static parquet.filter2.predicate.FilterApi.notEq;
import static parquet.filter2.predicate.FilterApi.or;
import static parquet.filter2.predicate.FilterApi.userDefined;
import static parquet.filter2.predicate.Operators.NotEq;

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

    assertTrue(outerAnd instanceof And);

    FilterPredicate not = ((And) outerAnd).getLeft();
    FilterPredicate gt = ((And) outerAnd).getRight();
    assertTrue(not instanceof Not);

    FilterPredicate or = ((Not) not).getPredicate();
    assertTrue(or instanceof Or);

    FilterPredicate leftEq = ((Or) or).getLeft();
    FilterPredicate rightNotEq = ((Or) or).getRight();
    assertTrue(leftEq instanceof Eq);
    assertTrue(rightNotEq instanceof NotEq);
    assertEquals(7, ((Eq) leftEq).getValue());
    assertEquals(17, ((NotEq) rightNotEq).getValue());
    assertEquals(ColumnPath.get("a", "b", "c"), ((Eq) leftEq).getColumn().getColumnPath());
    assertEquals(ColumnPath.get("a", "b", "c"), ((NotEq) rightNotEq).getColumn().getColumnPath());

    assertTrue(gt instanceof Gt);
    assertEquals(100.0, ((Gt) gt).getValue());
    assertEquals(ColumnPath.get("x", "y", "z"), ((Gt) gt).getColumn().getColumnPath());
  }

  @Test
  public void testToString() {
    FilterPredicate pred = or(predicate, notEq(binColumn, Binary.fromString("foobarbaz")));
    assertEquals("or(and(not(or(eq(a.b.c, 7), noteq(a.b.c, 17))), gt(x.y.z, 100.0)), "
        + "noteq(a.string.column, Binary{\"foobarbaz\"}))",
        pred.toString());
  }

  @Test
  public void testUdp() {
    FilterPredicate predicate = or(eq(doubleColumn, 12.0), userDefined(intColumn, DummyUdp.class));
    assertTrue(predicate instanceof Or);
    FilterPredicate ud = ((Or) predicate).getRight();
    assertTrue(ud instanceof UserDefinedByClass);
    assertEquals(DummyUdp.class, ((UserDefinedByClass) ud).getUserDefinedPredicateClass());
    assertTrue(((UserDefined) ud).getUserDefinedPredicate() instanceof DummyUdp);
  }

  @Test
  public void testSerializable() throws Exception {    
    BinaryColumn binary = binaryColumn("foo");
    FilterPredicate p = and(or(and(userDefined(intColumn, DummyUdp.class), predicate), eq(binary, Binary.fromString("hi"))), userDefined(longColumn, new IsMultipleOf(7)));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(p);
    oos.close();

    ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    FilterPredicate read = (FilterPredicate) is.readObject();
    assertEquals(p, read);
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
