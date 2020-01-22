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
package org.apache.parquet.glob;

import org.junit.Test;

import static org.junit.Assert.fail;

public class TestWildcardPath {

  private static void assertMatches(WildcardPath wp, String... strings) {
    for (String s : strings) {
      if (!wp.matches(s)) {
        fail(String.format("String '%s' was expected to match '%s'", s, wp));
      }
    }
  }

  private static void assertDoesNotMatch(WildcardPath wp, String... strings) {
    for (String s : strings) {
      if (wp.matches(s)) {
        fail(String.format("String '%s' was not expected to match '%s'", s, wp));
      }
    }
  }

  @Test
  public void testNoWildcards() {
    WildcardPath wp = new WildcardPath("", "foo", '.');
    assertMatches(wp, "foo", "foo.x", "foo.x.y");
    assertDoesNotMatch(wp, "xfoo", "xfoox", "fooa.x.y");
  }

  @Test
  public void testStarMatchesEverything() {
    WildcardPath wp = new WildcardPath("", "*", '.');
    assertMatches(wp, "", ".", "hi", "foo.bar", "*", "foo.");
  }

  @Test
  public void testChildrenPathsMatch() {
    WildcardPath wp = new WildcardPath("", "x.y.z", '.');
    assertMatches(wp, "x.y.z", "x.y.z.bar", "x.y.z.bar.baz.bop");
    assertDoesNotMatch(wp, "x.y.zzzz", "x.y.b", "x.y.a.z", "x.y.zhi.z");
  }

  @Test
  public void testEmptyString() {
    WildcardPath wp = new WildcardPath("", "", '.');
    assertMatches(wp, "");
    assertDoesNotMatch(wp, "x");
  }

  @Test
  public void testDoubleStarsIgnored() {
    WildcardPath wp = new WildcardPath("", "foo**bar", '.');
    assertMatches(wp, "foobar", "fooxyzbar", "foo.x.y.z.bar");
    assertDoesNotMatch(wp, "fobar", "hi", "foobazr");

    wp = new WildcardPath("", "foo********bar", '.');
    assertMatches(wp, "foobar", "fooxyzbar", "foo.x.y.z.bar");
    assertDoesNotMatch(wp, "fobar", "hi", "foobazr");
  }

  @Test
  public void testStarsAtBeginAndEnd() {
    WildcardPath wp = new WildcardPath("", "*x.y.z", '.');
    assertMatches(wp, "a.b.c.x.y.z", "x.y.z", "zoopx.y.z", "zoopx.y.z.child");
    assertDoesNotMatch(wp, "a.b.c.x.y", "xy.z", "hi");

    wp = new WildcardPath("", "*.x.y.z", '.');
    assertMatches(wp, "a.b.c.x.y.z", "foo.x.y.z", "foo.x.y.z.child");
    assertDoesNotMatch(wp, "x.y.z", "a.b.c.x.y", "xy.z", "hi", "zoopx.y.z", "zoopx.y.z.child");

    wp = new WildcardPath("", "x.y.z*", '.');
    assertMatches(wp, "x.y.z", "x.y.z.foo", "x.y.zoo", "x.y.zoo.bar");
    assertDoesNotMatch(wp, "a.b.c.x.y.z", "foo.x.y.z", "hi");

    wp = new WildcardPath("", "x.y.z.*", '.');
    assertMatches(wp, "x.y.z.foo", "x.y.z.bar.baz");
    assertDoesNotMatch(wp, "x.y.z", "a.b.c.x.y.z", "x.y.zoo", "foo.x.y.z", "hi", "x.y.zoo.bar");
  }

  @Test
  public void testComplex() {
    WildcardPath wp = new WildcardPath("", "*.street", '.');
    assertMatches(wp, "home.address.street", "home.address.street.number", "work.address.street",
        "work.address.street.foo", "street.street", "street.street.street.street", "thing.street.thing");

    assertDoesNotMatch(wp, "home.address.street_2", "home.address.street_2.number", "work.addressstreet",
        "work.addressstreet.foo", "", "x.y.z.street2", "x.y.z.street2.z");

    wp = new WildcardPath("", "x.y.*_stat.average", '.');
    assertMatches(wp, "x.y.z_stat.average", "x.y.foo_stat.average", "x.y.z.a.b_stat.average",
        "x.y.z.a.b_stat.average.child", "x.y.z._stat.average");
    assertDoesNotMatch(wp, "x.y.z_stats.average", "x.y.z_stat.averages", "x.y_stat.average", "x.yyy.foo_stat.average");

    wp = new WildcardPath("", "x.y.pre*.bar", '.');
    assertMatches(wp, "x.y.pre.bar", "x.y.preabc.bar", "x.y.prebar.bar");
    assertDoesNotMatch(wp, "x.y.pre.baraaaa", "x.y.preabc.baraaaa");
  }
}
