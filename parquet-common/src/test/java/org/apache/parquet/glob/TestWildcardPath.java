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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TestWildcardPath {

  @Test
  public void testNoWildcards() {
    WildcardPath wp = new WildcardPath("", "foo", '.');

    assertThat(wp.matches("foo")).isTrue();
    assertThat(wp.matches("foo.x")).isTrue();
    assertThat(wp.matches("foo.x.y")).isTrue();

    assertThat(wp.matches("xfoo")).isFalse();
    assertThat(wp.matches("xfoox")).isFalse();
    assertThat(wp.matches("fooa.x.y")).isFalse();
  }

  @Test
  public void testStarMatchesEverything() {
    WildcardPath wp = new WildcardPath("", "*", '.');

    assertThat(wp.matches("")).isTrue();
    assertThat(wp.matches(".")).isTrue();
    assertThat(wp.matches("hi")).isTrue();
    assertThat(wp.matches("foo.bar")).isTrue();
    assertThat(wp.matches("*")).isTrue();
    assertThat(wp.matches("foo.")).isTrue();
  }

  @Test
  public void testChildrenPathsMatch() {
    WildcardPath wp = new WildcardPath("", "x.y.z", '.');

    assertThat(wp.matches("x.y.z")).isTrue();
    assertThat(wp.matches("x.y.z.bar")).isTrue();
    assertThat(wp.matches("x.y.z.bar.baz.bop")).isTrue();

    assertThat(wp.matches("x.y.zzzz")).isFalse();
    assertThat(wp.matches("x.y.b")).isFalse();
    assertThat(wp.matches("x.y.a.z")).isFalse();
    assertThat(wp.matches("x.y.zhi.z")).isFalse();
  }

  @Test
  public void testEmptyString() {
    WildcardPath wp = new WildcardPath("", "", '.');

    assertThat(wp.matches("")).isTrue();
    assertThat(wp.matches("x")).isFalse();
  }

  @Test
  public void testDoubleStarsIgnored() {
    WildcardPath wp = new WildcardPath("", "foo**bar", '.');

    assertThat(wp.matches("foobar")).isTrue();
    assertThat(wp.matches("fooxyzbar")).isTrue();
    assertThat(wp.matches("foo.x.y.z.bar")).isTrue();

    assertThat(wp.matches("fobar")).isFalse();
    assertThat(wp.matches("hi")).isFalse();
    assertThat(wp.matches("foobazr")).isFalse();

    wp = new WildcardPath("", "foo********bar", '.');

    assertThat(wp.matches("foobar")).isTrue();
    assertThat(wp.matches("fooxyzbar")).isTrue();
    assertThat(wp.matches("foo.x.y.z.bar")).isTrue();

    assertThat(wp.matches("fobar")).isFalse();
    assertThat(wp.matches("hi")).isFalse();
    assertThat(wp.matches("foobazr")).isFalse();
  }

  @Test
  public void testStarsAtBeginAndEnd() {
    WildcardPath wp = new WildcardPath("", "*x.y.z", '.');

    assertThat(wp.matches("a.b.c.x.y.z")).isTrue();
    assertThat(wp.matches("x.y.z")).isTrue();
    assertThat(wp.matches("zoopx.y.z")).isTrue();
    assertThat(wp.matches("zoopx.y.z.child")).isTrue();

    assertThat(wp.matches("a.b.c.x.y")).isFalse();
    assertThat(wp.matches("xy.z")).isFalse();
    assertThat(wp.matches("hi")).isFalse();

    wp = new WildcardPath("", "*.x.y.z", '.');

    assertThat(wp.matches("a.b.c.x.y.z")).isTrue();
    assertThat(wp.matches("foo.x.y.z")).isTrue();
    assertThat(wp.matches("foo.x.y.z.child")).isTrue();

    assertThat(wp.matches("x.y.z")).isFalse();
    assertThat(wp.matches("a.b.c.x.y")).isFalse();
    assertThat(wp.matches("xy.z")).isFalse();
    assertThat(wp.matches("hi")).isFalse();
    assertThat(wp.matches("zoopx.y.z")).isFalse();
    assertThat(wp.matches("zoopx.y.z.child")).isFalse();

    wp = new WildcardPath("", "x.y.z*", '.');

    assertThat(wp.matches("x.y.z")).isTrue();
    assertThat(wp.matches("x.y.z.foo")).isTrue();
    assertThat(wp.matches("x.y.zoo")).isTrue();
    assertThat(wp.matches("x.y.zoo.bar")).isTrue();

    assertThat(wp.matches("a.b.c.x.y.z")).isFalse();
    assertThat(wp.matches("foo.x.y.z")).isFalse();
    assertThat(wp.matches("hi")).isFalse();

    wp = new WildcardPath("", "x.y.z.*", '.');

    assertThat(wp.matches("x.y.z.foo")).isTrue();
    assertThat(wp.matches("x.y.z.bar.baz")).isTrue();

    assertThat(wp.matches("x.y.z")).isFalse();
    assertThat(wp.matches("a.b.c.x.y.z")).isFalse();
    assertThat(wp.matches("x.y.zoo")).isFalse();
    assertThat(wp.matches("foo.x.y.z")).isFalse();
    assertThat(wp.matches("hi")).isFalse();
    assertThat(wp.matches("x.y.zoo.bar")).isFalse();
  }

  @Test
  public void testComplex() {
    WildcardPath wp = new WildcardPath("", "*.street", '.');

    assertThat(wp.matches("home.address.street")).isTrue();
    assertThat(wp.matches("home.address.street.number")).isTrue();
    assertThat(wp.matches("work.address.street")).isTrue();
    assertThat(wp.matches("work.address.street.foo")).isTrue();
    assertThat(wp.matches("street.street")).isTrue();
    assertThat(wp.matches("street.street.street.street")).isTrue();
    assertThat(wp.matches("thing.street.thing")).isTrue();

    assertThat(wp.matches("home.address.street_2")).isFalse();
    assertThat(wp.matches("home.address.street_2.number")).isFalse();
    assertThat(wp.matches("work.addressstreet")).isFalse();
    assertThat(wp.matches("work.addressstreet.foo")).isFalse();
    assertThat(wp.matches("")).isFalse();
    assertThat(wp.matches("x.y.z.street2")).isFalse();
    assertThat(wp.matches("x.y.z.street2.z")).isFalse();

    wp = new WildcardPath("", "x.y.*_stat.average", '.');

    assertThat(wp.matches("x.y.z_stat.average")).isTrue();
    assertThat(wp.matches("x.y.foo_stat.average")).isTrue();
    assertThat(wp.matches("x.y.z.a.b_stat.average")).isTrue();
    assertThat(wp.matches("x.y.z.a.b_stat.average.child")).isTrue();
    assertThat(wp.matches("x.y.z._stat.average")).isTrue();

    assertThat(wp.matches("x.y.z_stats.average")).isFalse();
    assertThat(wp.matches("x.y.z_stat.averages")).isFalse();
    assertThat(wp.matches("x.y_stat.average")).isFalse();
    assertThat(wp.matches("x.yyy.foo_stat.average")).isFalse();

    wp = new WildcardPath("", "x.y.pre*.bar", '.');

    assertThat(wp.matches("x.y.pre.bar")).isTrue();
    assertThat(wp.matches("x.y.preabc.bar")).isTrue();
    assertThat(wp.matches("x.y.prebar.bar")).isTrue();

    assertThat(wp.matches("x.y.pre.baraaaa")).isFalse();
    assertThat(wp.matches("x.y.preabc.baraaaa")).isFalse();
  }
}
