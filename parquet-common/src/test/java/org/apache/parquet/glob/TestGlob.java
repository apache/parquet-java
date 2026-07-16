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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.parquet.Strings;
import org.apache.parquet.glob.GlobParser.GlobParseException;
import org.junit.jupiter.api.Test;

public class TestGlob {

  @Test
  public void testNoGlobs() {
    assertThat(Strings.expandGlob("foo")).containsExactly("foo");
  }

  @Test
  public void testEmptyGroup() {
    assertThat(Strings.expandGlob("")).containsExactly("");
    assertThat(Strings.expandGlob("{}")).containsExactly("");
    assertThat(Strings.expandGlob("a{}")).containsExactly("a");
    assertThat(Strings.expandGlob("a{}b")).containsExactly("ab");
    assertThat(Strings.expandGlob("{}a")).containsExactly("a");
    assertThat(Strings.expandGlob("a{}")).containsExactly("a");
    assertThat(Strings.expandGlob("{,}")).containsExactly("", "");
    assertThat(Strings.expandGlob("a{b,{},c}")).containsExactly("ab", "a", "ac");
  }

  @Test
  public void testSingleLevel() {
    assertThat(Strings.expandGlob("foo{bar,baz}")).containsExactly("foobar", "foobaz");
    assertThat(Strings.expandGlob("start{foo,bar}end")).containsExactly("startfooend", "startbarend");
    assertThat(Strings.expandGlob("{foo,bar}end")).containsExactly("fooend", "barend");
    assertThat(Strings.expandGlob("start{foo,bar}end{a,b,c,d}"))
        .containsExactly(
            "startfooenda",
            "startfooendb",
            "startfooendc",
            "startfooendd",
            "startbarenda",
            "startbarendb",
            "startbarendc",
            "startbarendd");
    assertThat(Strings.expandGlob("{x,y}{a,b,c}")).containsExactly("xa", "xb", "xc", "ya", "yb", "yc");
    assertThat(Strings.expandGlob("{x,y,z}")).containsExactly("x", "y", "z");
  }

  @Test
  public void testNested() {
    assertThat(Strings.expandGlob("{start{one,pre{two,three}post,{four,five}}end,a,b,foo{x,y}}"))
        .containsExactly(
            "startoneend",
            "startpretwopostend",
            "startprethreepostend",
            "startfourend",
            "startfiveend",
            "a",
            "b",
            "foox",
            "fooy");
  }

  @Test
  public void testExtraBraces() {
    assertThat(Strings.expandGlob("{{x,y,z}}")).containsExactly("x", "y", "z");
    assertThat(Strings.expandGlob("{{{x,y,z}}}")).containsExactly("x", "y", "z");
    assertThat(Strings.expandGlob("start{x,{a,b},y}")).containsExactly("startx", "starta", "startb", "starty");
  }

  @Test
  public void testCommaInTopLevel() {
    assertThatThrownBy(() -> Strings.expandGlob("foo,bar"))
        .isInstanceOf(GlobParseException.class)
        .hasMessage("Unexpected comma outside of a {} group:\n" + "foo,bar\n" + "---^");
  }

  @Test
  public void testCommaCornerCases() {
    // single empty string in each location
    assertThat(Strings.expandGlob("foo{bar,,baz}")).containsExactly("foobar", "foo", "foobaz");
    assertThat(Strings.expandGlob("foo{,bar,baz}")).containsExactly("foo", "foobar", "foobaz");
    assertThat(Strings.expandGlob("foo{bar,baz,}")).containsExactly("foobar", "foobaz", "foo");

    // multiple empty strings
    assertThat(Strings.expandGlob("foo{bar,,,baz}")).containsExactly("foobar", "foo", "foo", "foobaz");
    assertThat(Strings.expandGlob("foo{,,bar,baz}")).containsExactly("foo", "foo", "foobar", "foobaz");
    assertThat(Strings.expandGlob("foo{bar,baz,,}")).containsExactly("foobar", "foobaz", "foo", "foo");

    // between groups
    assertThat(Strings.expandGlob("{{x,y},,{a,b}}")).containsExactly("x", "y", "", "a", "b");
  }

  private void assertNotEnoughCloseBraces(String s) {
    assertThatThrownBy(() -> Strings.expandGlob(s))
        .isInstanceOf(GlobParseException.class)
        .hasMessageStartingWith("Not enough close braces in: ");
  }

  private void assertTooManyCloseBraces(String s) {
    assertThatThrownBy(() -> Strings.expandGlob(s))
        .isInstanceOf(GlobParseException.class)
        .hasMessageStartingWith("Unexpected closing }:");
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
