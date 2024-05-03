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

import static org.apache.parquet.filter2.predicate.ContainsRewriter.rewrite;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.contains;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.junit.Assert.assertEquals;

import org.apache.parquet.filter2.predicate.Operators.Contains;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;
import org.junit.Test;

public class TestContainsRewriter {
  private static final IntColumn intColumn = intColumn("a.b.c");
  private static final DoubleColumn doubleColumn = doubleColumn("a.b.c");

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

    Contains<Integer> containsLhs = contains(eq(intColumn, 17));
    Contains<Integer> containsRhs = contains(eq(intColumn, 7));

    assertNoOp(containsLhs);
    assertEquals(containsLhs.and(containsRhs), rewrite(and(containsLhs, containsRhs)));
    assertEquals(containsLhs.or(containsRhs), rewrite(or(containsLhs, containsRhs)));
  }

  @Test
  public void testNested() {
    Contains<Integer> contains1 = contains(eq(intColumn, 1));
    Contains<Integer> contains2 = contains(eq(intColumn, 2));
    Contains<Integer> contains3 = contains(eq(intColumn, 3));
    Contains<Integer> contains4 = contains(eq(intColumn, 4));

    assertEquals(contains1.and(contains2.or(contains3)), rewrite(and(contains1, or(contains2, contains3))));
    assertEquals(contains1.and(contains2).or(contains3), rewrite(or(and(contains1, contains2), contains3)));

    assertEquals(
        contains1.and(contains2).and(contains2.or(contains3)),
        rewrite(and(and(contains1, contains2), or(contains2, contains3))));
    assertEquals(
        contains1.and(contains2).or(contains3.or(contains4)),
        rewrite(or(and(contains1, contains2), or(contains3, contains4))));
  }
}
