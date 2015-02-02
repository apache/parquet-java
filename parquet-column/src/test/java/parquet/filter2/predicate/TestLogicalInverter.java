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
import static parquet.filter2.predicate.LogicalInverter.invert;

public class TestLogicalInverter {
  private static final IntColumn intColumn = intColumn("a.b.c");
  private static final DoubleColumn doubleColumn = doubleColumn("a.b.c");

  private  static  final UserDefined<Integer, DummyUdp> ud = userDefined(intColumn, DummyUdp.class);

  private static final FilterPredicate complex =
      and(
          or(ltEq(doubleColumn, 12.0),
              and(
                  not(or(eq(intColumn, 7), notEq(intColumn, 17))),
                  userDefined(intColumn, DummyUdp.class))),
          or(gt(doubleColumn, 100.0), notEq(intColumn, 77)));

  private static final FilterPredicate complexInverse =
      or(
          and(gt(doubleColumn, 12.0),
              or(
                  or(eq(intColumn, 7), notEq(intColumn, 17)),
                  new LogicalNotUserDefined<Integer, DummyUdp>(userDefined(intColumn, DummyUdp.class)))),
          and(ltEq(doubleColumn, 100.0), eq(intColumn, 77)));

  @Test
  public void testBaseCases() {
    assertEquals(notEq(intColumn, 17), invert(eq(intColumn, 17)));
    assertEquals(eq(intColumn, 17), invert(notEq(intColumn, 17)));
    assertEquals(gtEq(intColumn, 17), invert(lt(intColumn, 17)));
    assertEquals(gt(intColumn, 17), invert(ltEq(intColumn, 17)));
    assertEquals(ltEq(intColumn, 17), invert(gt(intColumn, 17)));
    assertEquals(lt(intColumn, 17), invert(gtEq(intColumn, 17)));

    FilterPredicate andPos = and(eq(intColumn, 17), eq(doubleColumn, 12.0));
    FilterPredicate andInv = or(notEq(intColumn, 17), notEq(doubleColumn, 12.0));
    assertEquals(andInv, invert(andPos));

    FilterPredicate orPos = or(eq(intColumn, 17), eq(doubleColumn, 12.0));
    FilterPredicate orInv = and(notEq(intColumn, 17), notEq(doubleColumn, 12.0));
    assertEquals(orPos, invert(orInv));

    assertEquals(eq(intColumn, 17), invert(not(eq(intColumn, 17))));

    UserDefined<Integer, DummyUdp> ud = userDefined(intColumn, DummyUdp.class);
    assertEquals(new LogicalNotUserDefined<Integer, DummyUdp>(ud), invert(ud));
    assertEquals(ud, invert(not(ud)));
    assertEquals(ud, invert(new LogicalNotUserDefined<Integer, DummyUdp>(ud)));
  }

  @Test
  public void testComplex() {
    assertEquals(complexInverse, invert(complex));
  }
}
