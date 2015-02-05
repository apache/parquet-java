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

import org.junit.Test;

import parquet.filter2.predicate.Operators.DoubleColumn;
import parquet.filter2.predicate.Operators.IntColumn;
import parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import parquet.filter2.predicate.Operators.UserDefined;

import static org.junit.Assert.assertEquals;
import static parquet.filter2.predicate.FilterApi.and;
import static parquet.filter2.predicate.FilterApi.doubleColumn;
import static parquet.filter2.predicate.FilterApi.eq;
import static parquet.filter2.predicate.FilterApi.gt;
import static parquet.filter2.predicate.FilterApi.gtEq;
import static parquet.filter2.predicate.FilterApi.intColumn;
import static parquet.filter2.predicate.FilterApi.lt;
import static parquet.filter2.predicate.FilterApi.ltEq;
import static parquet.filter2.predicate.FilterApi.not;
import static parquet.filter2.predicate.FilterApi.notEq;
import static parquet.filter2.predicate.FilterApi.or;
import static parquet.filter2.predicate.FilterApi.userDefined;
import static parquet.filter2.predicate.LogicalInverseRewriter.rewrite;

public class TestLogicalInverseRewriter {
  private static final IntColumn intColumn = intColumn("a.b.c");
  private static final DoubleColumn doubleColumn = doubleColumn("a.b.c");

  private static final FilterPredicate complex =
      and(
          not(
              or(ltEq(doubleColumn, 12.0),
                  and(
                      not(or(eq(intColumn, 7), notEq(intColumn, 17))),
                      userDefined(intColumn, DummyUdp.class)))),
          or(gt(doubleColumn, 100.0), not(gtEq(intColumn, 77))));

  private static final FilterPredicate complexCollapsed =
      and(
          and(gt(doubleColumn, 12.0),
              or(
                  or(eq(intColumn, 7), notEq(intColumn, 17)),
                  new LogicalNotUserDefined<Integer, DummyUdp>(userDefined(intColumn, DummyUdp.class)))),
          or(gt(doubleColumn, 100.0), lt(intColumn, 77)));

  private static void assertNoOp(FilterPredicate p) {
    assertEquals(p, rewrite(p));
  }

  @Test
  public void testBaseCases() {
    UserDefined<Integer, DummyUdp> ud = userDefined(intColumn, DummyUdp.class);

    assertNoOp(eq(intColumn, 17));
    assertNoOp(notEq(intColumn, 17));
    assertNoOp(lt(intColumn, 17));
    assertNoOp(ltEq(intColumn, 17));
    assertNoOp(gt(intColumn, 17));
    assertNoOp(gtEq(intColumn, 17));
    assertNoOp(and(eq(intColumn, 17), eq(doubleColumn, 12.0)));
    assertNoOp(or(eq(intColumn, 17), eq(doubleColumn, 12.0)));
    assertNoOp(ud);

    assertEquals(notEq(intColumn, 17), rewrite(not(eq(intColumn, 17))));
    assertEquals(eq(intColumn, 17), rewrite(not(notEq(intColumn, 17))));
    assertEquals(gtEq(intColumn, 17), rewrite(not(lt(intColumn, 17))));
    assertEquals(gt(intColumn, 17), rewrite(not(ltEq(intColumn, 17))));
    assertEquals(ltEq(intColumn, 17), rewrite(not(gt(intColumn, 17))));
    assertEquals(lt(intColumn, 17), rewrite(not(gtEq(intColumn, 17))));
    assertEquals(new LogicalNotUserDefined<Integer, DummyUdp>(ud), rewrite(not(ud)));

    FilterPredicate notedAnd = not(and(eq(intColumn, 17), eq(doubleColumn, 12.0)));
    FilterPredicate distributedAnd = or(notEq(intColumn, 17), notEq(doubleColumn, 12.0));
    assertEquals(distributedAnd, rewrite(notedAnd));

    FilterPredicate andWithNots = and(not(gtEq(intColumn, 17)), lt(intColumn, 7));
    FilterPredicate andWithoutNots = and(lt(intColumn, 17), lt(intColumn, 7));
    assertEquals(andWithoutNots, rewrite(andWithNots));
  }

  @Test
  public void testComplex() {
    assertEquals(complexCollapsed, rewrite(complex));
  }
}
