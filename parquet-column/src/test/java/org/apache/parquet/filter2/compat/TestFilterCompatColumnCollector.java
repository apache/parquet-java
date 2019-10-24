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
package org.apache.parquet.filter2.compat;

import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter.AndRecordFilter;
import org.apache.parquet.filter.ColumnPredicates;
import org.apache.parquet.filter.ColumnRecordFilter;
import org.apache.parquet.filter.NotRecordFilter;
import org.apache.parquet.filter.OrRecordFilter;
import org.apache.parquet.filter.PagedRecordFilter;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.predicate.DummyUdp;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestFilterCompatColumnCollector {

  @Test
  public void testVisitFilterPredicateCompat() {
    FilterPredicate f1 = or(eq(intColumn("a"), 1),
                            notEq(intColumn("a"), 2));
    FilterPredicate f2 = and(gt(intColumn("b.b"), 1),
                             gtEq(intColumn("b.b"), 2));
    FilterPredicate f3 = not(lt(intColumn("c.c1"), 1));
    FilterPredicate f4 = not(ltEq(intColumn("c.c2"), 2));
    FilterPredicate f5 = userDefined(intColumn("d.d.d1"), DummyUdp.class);
    FilterPredicate f6 = not(userDefined(intColumn("d.d.d2"), DummyUdp.class));
    FilterPredicate f = and(f1, and(f2, and(f3, and(f4, and(f5, f6)))));

    Set<ColumnPath> resultSet =
        FilterCompatColumnCollector.INSTANCE.visit((FilterCompat.FilterPredicateCompat) (FilterCompat.get(f)));

    Set<ColumnPath> expectSet = new HashSet<ColumnPath>();
    expectSet.add(ColumnPath.get("a"));
    expectSet.add(ColumnPath.get("b", "b"));
    expectSet.add(ColumnPath.get("c", "c1"));
    expectSet.add(ColumnPath.get("c", "c2"));
    expectSet.add(ColumnPath.get("d", "d", "d1"));
    expectSet.add(ColumnPath.get("d", "d", "d2"));

    assertEquals(expectSet, resultSet);
  }

  private static class DummyColumnPredicate implements ColumnPredicates.Predicate {

    @Override
    public boolean apply(ColumnReader input) {
      return false;
    }
  }

  @Test
  public void testVisitUnboundRecordFilterCompat() {
    UnboundRecordFilter f1 = ColumnRecordFilter.column("a", new DummyColumnPredicate());
    UnboundRecordFilter f2 = ColumnRecordFilter.column("a", new DummyColumnPredicate());
    UnboundRecordFilter f3 = ColumnRecordFilter.column("b.b", new DummyColumnPredicate());
    UnboundRecordFilter f4 = ColumnRecordFilter.column("b.b", new DummyColumnPredicate());
    UnboundRecordFilter f5 = ColumnRecordFilter.column("c.c1", new DummyColumnPredicate());
    UnboundRecordFilter f6 = ColumnRecordFilter.column("c.c2", new DummyColumnPredicate());
    UnboundRecordFilter f7 = ColumnRecordFilter.column("d.d.d1", new DummyColumnPredicate());
    UnboundRecordFilter f8 = ColumnRecordFilter.column("d.d.d2", new DummyColumnPredicate());

    UnboundRecordFilter f9 = OrRecordFilter.or(f1, OrRecordFilter.or(f2, OrRecordFilter.or(f3, f4)));
    UnboundRecordFilter f10 =
        NotRecordFilter.not(OrRecordFilter.or(f5, OrRecordFilter.or(f6, OrRecordFilter.or(f7, f8))));
    UnboundRecordFilter f = AndRecordFilter.and(AndRecordFilter.and(f9, f10), PagedRecordFilter.page(0, 100));

    Set<ColumnPath> resultSet =
        FilterCompatColumnCollector.INSTANCE.visit((FilterCompat.UnboundRecordFilterCompat) (FilterCompat.get(f)));

    Set<ColumnPath> expectSet = new HashSet<ColumnPath>();
    expectSet.add(ColumnPath.get("a"));
    expectSet.add(ColumnPath.get("b", "b"));
    expectSet.add(ColumnPath.get("c", "c1"));
    expectSet.add(ColumnPath.get("c", "c2"));
    expectSet.add(ColumnPath.get("d", "d", "d1"));
    expectSet.add(ColumnPath.get("d", "d", "d2"));

    assertEquals(expectSet, resultSet);
  }

  @Test
  public void testVisitNoOpFilter() {
    assertNull(FilterCompatColumnCollector.INSTANCE.visit((FilterCompat.NoOpFilter) FilterCompat.NOOP));
  }
}