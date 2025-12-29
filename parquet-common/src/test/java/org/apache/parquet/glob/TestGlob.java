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

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.util.List;
import junit.framework.Assert;
import org.apache.parquet.Strings;
import org.apache.parquet.glob.GlobParser.GlobParseException;
import org.junit.Test;

public class TestGlob {

  @Test
  public void testNoGlobs() {
    assertEquals(List.of("foo"), Strings.expandGlob("foo"));
  }

  @Test
  public void testEmptyGroup() {
    assertEquals(List.of(""), Strings.expandGlob(""));
    assertEquals(List.of(""), Strings.expandGlob("{}"));
    assertEquals(List.of("a"), Strings.expandGlob("a{}"));
    assertEquals(List.of("ab"), Strings.expandGlob("a{}b"));
    assertEquals(List.of("a"), Strings.expandGlob("{}a"));
    assertEquals(List.of("a"), Strings.expandGlob("a{}"));
    assertEquals(List.of("", ""), Strings.expandGlob("{,}"));
    assertEquals(List.of("ab", "a", "ac"), Strings.expandGlob("a{b,{},c}"));
  }

  @Test
  public void testSingleLevel() {
    assertEquals(List.of("foobar", "foobaz"), Strings.expandGlob("foo{bar,baz}"));
    assertEquals(List.of("startfooend", "startbarend"), Strings.expandGlob("start{foo,bar}end"));
    assertEquals(List.of("fooend", "barend"), Strings.expandGlob("{foo,bar}end"));
    assertEquals(
        List.of(
            "startfooenda",
            "startfooendb",
            "startfooendc",
            "startfooendd",
            "startbarenda",
            "startbarendb",
            "startbarendc",
            "startbarendd"),
        Strings.expandGlob("start{foo,bar}end{a,b,c,d}"));
    assertEquals(List.of("xa", "xb", "xc", "ya", "yb", "yc"), Strings.expandGlob("{x,y}{a,b,c}"));
    assertEquals(List.of("x", "y", "z"), Strings.expandGlob("{x,y,z}"));
  }

  @Test
  public void testNested() {
    assertEquals(
        List.of(
            "startoneend",
            "startpretwopostend",
            "startprethreepostend",
            "startfourend",
            "startfiveend",
            "a",
            "b",
            "foox",
            "fooy"),
        Strings.expandGlob("{start{one,pre{two,three}post,{four,five}}end,a,b,foo{x,y}}"));
  }

  @Test
  public void testExtraBraces() {
    assertEquals(List.of("x", "y", "z"), Strings.expandGlob("{{x,y,z}}"));
    assertEquals(List.of("x", "y", "z"), Strings.expandGlob("{{{x,y,z}}}"));
    assertEquals(List.of("startx", "starta", "startb", "starty"), Strings.expandGlob("start{x,{a,b},y}"));
  }

  @Test
  public void testCommaInTopLevel() {
    try {
      Strings.expandGlob("foo,bar");
      fail("This should throw");
    } catch (GlobParseException e) {
      Assert.assertEquals("Unexpected comma outside of a {} group:\n" + "foo,bar\n" + "---^", e.getMessage());
    }
  }

  @Test
  public void testCommaCornerCases() {
    // single empty string in each location
    assertEquals(List.of("foobar", "foo", "foobaz"), Strings.expandGlob("foo{bar,,baz}"));
    assertEquals(List.of("foo", "foobar", "foobaz"), Strings.expandGlob("foo{,bar,baz}"));
    assertEquals(List.of("foobar", "foobaz", "foo"), Strings.expandGlob("foo{bar,baz,}"));

    // multiple empty strings
    assertEquals(List.of("foobar", "foo", "foo", "foobaz"), Strings.expandGlob("foo{bar,,,baz}"));
    assertEquals(List.of("foo", "foo", "foobar", "foobaz"), Strings.expandGlob("foo{,,bar,baz}"));
    assertEquals(List.of("foobar", "foobaz", "foo", "foo"), Strings.expandGlob("foo{bar,baz,,}"));

    // between groups
    assertEquals(List.of("x", "y", "", "a", "b"), Strings.expandGlob("{{x,y},,{a,b}}"));
  }

  private void assertNotEnoughCloseBraces(String s) {
    String expected = "Not enough close braces in: ";
    try {
      Strings.expandGlob(s);
      fail("this should throw");
    } catch (GlobParseException e) {
      Assert.assertEquals(expected, e.getMessage().substring(0, expected.length()));
    }
  }

  private void assertTooManyCloseBraces(String s) {
    String expected = "Unexpected closing }:";
    try {
      Strings.expandGlob(s);
      fail("this should throw");
    } catch (GlobParseException e) {
      Assert.assertEquals(expected, e.getMessage().substring(0, expected.length()));
    }
  }

  @Test
  public void testMismatchedBraces() {
    assertNotEnoughCloseBraces("{");
    assertNotEnoughCloseBraces("{}{}{}{{}{}{");
    assertNotEnoughCloseBraces("foo{bar");
    assertNotEnoughCloseBraces("foo{{bar}");
    assertNotEnoughCloseBraces("foo{}{{bar}");

    assertTooManyCloseBraces("{}}{");
    assertTooManyCloseBraces("}");
    assertTooManyCloseBraces("{}{}{}}{}{}{");
    assertTooManyCloseBraces("foo}bar");
    assertTooManyCloseBraces("foo}}bar}");
    assertTooManyCloseBraces("foo{}{{bar}}}");
  }
}
