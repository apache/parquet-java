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
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.filter2.predicate.LogicalInverseRewriter.rewrite;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;
import org.junit.jupiter.api.Test;

public class TestLogicalInverseRewriter {
  private static final IntColumn intColumn = intColumn("a.b.c");
  private static final DoubleColumn doubleColumn = doubleColumn("a.b.c");

  private static final FilterPredicate complex = and(
      not(or(
          ltEq(doubleColumn, 12.0),
          and(not(or(eq(intColumn, 7), notEq(intColumn, 17))), userDefined(intColumn, DummyUdp.class)))),
      or(gt(doubleColumn, 100.0), not(gtEq(intColumn, 77))));

  private static final FilterPredicate complexCollapsed = and(
      and(
          gt(doubleColumn, 12.0),
          or(
              or(eq(intColumn, 7), notEq(intColumn, 17)),
              new LogicalNotUserDefined<>(userDefined(intColumn, DummyUdp.class)))),
      or(gt(doubleColumn, 100.0), lt(intColumn, 77)));

  private static void assertNoOp(FilterPredicate p) {
    assertThat(rewrite(p)).isEqualTo(p);
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

    assertThat(rewrite(not(eq(intColumn, 17)))).isEqualTo(notEq(intColumn, 17));
    assertThat(rewrite(not(notEq(intColumn, 17)))).isEqualTo(eq(intColumn, 17));
    assertThat(rewrite(not(lt(intColumn, 17)))).isEqualTo(gtEq(intColumn, 17));
    assertThat(rewrite(not(ltEq(intColumn, 17)))).isEqualTo(gt(intColumn, 17));
    assertThat(rewrite(not(gt(intColumn, 17)))).isEqualTo(ltEq(intColumn, 17));
    assertThat(rewrite(not(gtEq(intColumn, 17)))).isEqualTo(lt(intColumn, 17));
    assertThat(rewrite(not(ud))).isEqualTo(new LogicalNotUserDefined<>(ud));

    FilterPredicate notedAnd = not(and(eq(intColumn, 17), eq(doubleColumn, 12.0)));
    FilterPredicate distributedAnd = or(notEq(intColumn, 17), notEq(doubleColumn, 12.0));
    assertThat(rewrite(notedAnd)).isEqualTo(distributedAnd);

    FilterPredicate andWithNots = and(not(gtEq(intColumn, 17)), lt(intColumn, 7));
    FilterPredicate andWithoutNots = and(lt(intColumn, 17), lt(intColumn, 7));
    assertThat(rewrite(andWithNots)).isEqualTo(andWithoutNots);
  }

  @Test
  public void testComplex() {
    assertThat(rewrite(complex)).isEqualTo(complexCollapsed);
  }
}
