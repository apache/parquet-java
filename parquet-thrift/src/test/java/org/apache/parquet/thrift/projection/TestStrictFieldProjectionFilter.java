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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import org.junit.Test;

public class TestStrictFieldProjectionFilter {

  @Test
  public void testFromSemicolonDelimitedString() {
    assertThat(StrictFieldProjectionFilter.parseSemicolonDelimitedString(";x.y.z;*.a.b.c*;;foo;;;;bar;"))
        .containsExactly("x.y.z", "*.a.b.c*", "foo", "bar");

    assertThatThrownBy(() -> StrictFieldProjectionFilter.parseSemicolonDelimitedString(";;"))
        .isInstanceOf(ThriftProjectionException.class)
        .hasMessage("Semicolon delimited string ';;' contains 0 glob strings");
  }

  @Test
  public void testProjection() {
    StrictFieldProjectionFilter filter = StrictFieldProjectionFilter.fromSemicolonDelimitedString(
        "home.phone_number;home.address;work.address.zip;base_info;*.average;a.b.c.pre{x,y,z{a,b,c}}post");

    assertThat(filter.keep("home.phone_number")).isTrue();
    assertThat(filter.keep("home.address")).isTrue();
    assertThat(filter.keep("work.address.zip")).isTrue();
    assertThat(filter.keep("base_info")).isTrue();
    assertThat(filter.keep("foo.average")).isTrue();
    assertThat(filter.keep("bar.x.y.z.average")).isTrue();
    assertThat(filter.keep("base_info.nested.field")).isTrue();
    assertThat(filter.keep("a.b.c.prexpost")).isTrue();
    assertThat(filter.keep("a.b.c.prezapost")).isTrue();

    assertThat(filter.keep("home2.phone_number")).isFalse();
    assertThat(filter.keep("home2.address")).isFalse();
    assertThat(filter.keep("work.address")).isFalse();
    assertThat(filter.keep("base_info2")).isFalse();
    assertThat(filter.keep("foo_average")).isFalse();
    assertThat(filter.keep("bar.x.y.z_average")).isFalse();
    assertThat(filter.keep("base_info_nested.field")).isFalse();
    assertThat(filter.keep("hi")).isFalse();
    assertThat(filter.keep("average")).isFalse();
    assertThat(filter.keep("a.b.c.pre{x,y,z{a,b,c}}post")).isFalse();
    assertThat(filter.keep("")).isFalse();
  }

  @Test
  public void testIsStrict() {
    StrictFieldProjectionFilter filter = StrictFieldProjectionFilter.fromSemicolonDelimitedString(
        "home.phone_number;a.b.c.pre{x,y,z{a,b,c}}post;bar.*.average");

    assertThat(filter.keep("home.phone_number")).isTrue();
    assertThat(filter.keep("bar.foo.average")).isTrue();
    assertThat(filter.keep("a.b.c.prexpost")).isTrue();
    assertThat(filter.keep("a.b.c.prezcpost")).isTrue();
    assertThat(filter.keep("hello")).isFalse();

    assertThatThrownBy(filter::assertNoUnmatchedPatterns)
        .isInstanceOf(ThriftProjectionException.class)
        .hasMessage("The following projection patterns did not match any columns in this schema:\n"
            + "Pattern: 'a.b.c.pre{x,y,z{a,b,c}}post' (when expanded to 'a.b.c.preypost')\n"
            + "Pattern: 'a.b.c.pre{x,y,z{a,b,c}}post' (when expanded to 'a.b.c.prezapost')\n"
            + "Pattern: 'a.b.c.pre{x,y,z{a,b,c}}post' (when expanded to 'a.b.c.prezbpost')\n");
  }

  @Test
  public void testWarnWhenMultiplePatternsMatch() {
    StrictFieldProjectionFilter filter =
        spy(new StrictFieldProjectionFilter(Arrays.asList("a.b.c.{x_average,z_average}", "a.*_average")));
    doNothing().when(filter).warn(anyString());

    assertThat(filter.keep("a.b.c.x_average")).isTrue();
    assertThat(filter.keep("a.b.c.z_average")).isTrue();
    assertThat(filter.keep("a.other.w_average")).isTrue();
    assertThat(filter.keep("hello")).isFalse();

    verify(filter)
        .warn("Field path: 'a.b.c.x_average' matched more than one glob path pattern. "
            + "First match: 'a.b.c.{x_average,z_average}' (when expanded to 'a.b.c.x_average') "
            + "second match:'a.*_average' (when expanded to 'a.*_average')");
    verify(filter)
        .warn("Field path: 'a.b.c.z_average' matched more than one glob path pattern. "
            + "First match: 'a.b.c.{x_average,z_average}' (when expanded to 'a.b.c.z_average') "
            + "second match:'a.*_average' (when expanded to 'a.*_average')");
  }
}
