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
package org.apache.parquet.thrift.projection;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.easymock.EasyMock.createMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestStrictFieldProjectionFilter {

  @Test
  public void testFromSemicolonDelimitedString() {
    List<String> globs = StrictFieldProjectionFilter.parseSemicolonDelimitedString(";x.y.z;*.a.b.c*;;foo;;;;bar;");
    assertEquals(Arrays.asList("x.y.z", "*.a.b.c*", "foo", "bar"), globs);

    try {
      StrictFieldProjectionFilter.parseSemicolonDelimitedString(";;");
      fail("this should throw");
    } catch (ThriftProjectionException e) {
      assertEquals("Semicolon delimited string ';;' contains 0 glob strings", e.getMessage());
    }
  }

  private static void assertMatches(StrictFieldProjectionFilter filter, String... strings) {
    for (String s : strings) {
      if (!filter.keep(s)) {
        fail(String.format("String '%s' was expected to match", s));
      }
    }
  }

  private static void assertDoesNotMatch(StrictFieldProjectionFilter filter, String... strings) {
    for (String s : strings) {
      if (filter.keep(s)) {
        fail(String.format("String '%s' was not expected to match", s));
      }
    }
  }


  @Test
  public void testProjection() {
    StrictFieldProjectionFilter filter = StrictFieldProjectionFilter.fromSemicolonDelimitedString(
        "home.phone_number;home.address;work.address.zip;base_info;*.average;a.b.c.pre{x,y,z{a,b,c}}post");

    assertMatches(filter, "home.phone_number", "home.address", "work.address.zip", "base_info",
        "foo.average", "bar.x.y.z.average", "base_info.nested.field", "a.b.c.prexpost", "a.b.c.prezapost");

    assertDoesNotMatch(filter, "home2.phone_number", "home2.address", "work.address", "base_info2",
        "foo_average", "bar.x.y.z_average", "base_info_nested.field", "hi", "average", "a.b.c.pre{x,y,z{a,b,c}}post",
        "");

  }

  @Test
  public void testIsStrict() {
    StrictFieldProjectionFilter filter = StrictFieldProjectionFilter.fromSemicolonDelimitedString(
        "home.phone_number;a.b.c.pre{x,y,z{a,b,c}}post;bar.*.average");

    assertMatches(filter, "home.phone_number", "bar.foo.average", "a.b.c.prexpost", "a.b.c.prezcpost");
    assertDoesNotMatch(filter, "hello");
    try {
      filter.assertNoUnmatchedPatterns();
      fail("this should throw");
    } catch (ThriftProjectionException e) {
      String expectedMessage = "The following projection patterns did not match any columns in this schema:\n" +
          "Pattern: 'a.b.c.pre{x,y,z{a,b,c}}post' (when expanded to 'a.b.c.preypost')\n" +
          "Pattern: 'a.b.c.pre{x,y,z{a,b,c}}post' (when expanded to 'a.b.c.prezapost')\n" +
          "Pattern: 'a.b.c.pre{x,y,z{a,b,c}}post' (when expanded to 'a.b.c.prezbpost')\n";
      assertEquals(expectedMessage, e.getMessage());
    }
  }

  @Test
  public void testWarnWhenMultiplePatternsMatch() {
    StrictFieldProjectionFilter filter = createMockBuilder(StrictFieldProjectionFilter.class)
        .withConstructor(Arrays.asList("a.b.c.{x_average,z_average}", "a.*_average"))
        .addMockedMethod("warn")
        .createMock();

    // set expectations
    filter.warn("Field path: 'a.b.c.x_average' matched more than one glob path pattern. "
        + "First match: 'a.b.c.{x_average,z_average}' (when expanded to 'a.b.c.x_average') "
        + "second match:'a.*_average' (when expanded to 'a.*_average')");
    filter.warn("Field path: 'a.b.c.z_average' matched more than one glob path pattern. "
        + "First match: 'a.b.c.{x_average,z_average}' (when expanded to 'a.b.c.z_average') "
        + "second match:'a.*_average' (when expanded to 'a.*_average')");

    replay(filter);

    assertMatches(filter, "a.b.c.x_average", "a.b.c.z_average", "a.other.w_average");
    assertDoesNotMatch(filter, "hello");
    verify(filter);
  }

}
