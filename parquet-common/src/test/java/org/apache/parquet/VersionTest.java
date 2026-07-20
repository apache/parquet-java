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
package org.apache.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.VersionParser.VersionParseException;
import org.junit.jupiter.api.Test;

/**
 * This test doesn't do much, but it makes sure that the Version class
 * was properly generated, and that it's VERSION_NUMBER field has been
 * populated correctly. The hope is to catch any issues like the version
 * being an empty string or something along those lines.
 */
public class VersionTest {

  private void assertVersionValid(String v) {
    try {
      org.semver.Version.parse(v);
    } catch (RuntimeException e) {
      throw new RuntimeException(v + " is not a valid semver!", e);
    }
  }

  @Test
  public void testVersion() {
    assertVersionValid(Version.VERSION_NUMBER);
  }

  @Test
  public void testFullVersion() throws Exception {
    ParsedVersion version = VersionParser.parse(Version.FULL_VERSION);

    assertVersionValid(version.version);
    assertThat(version.version).isEqualTo(Version.VERSION_NUMBER);
    assertThat(version.application).isEqualTo("parquet-mr");
  }

  @Test
  public void testVersionParser() throws Exception {
    assertThat(VersionParser.parse("parquet-mr version 1.6.0 (build abcd)"))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.6.0", "abcd"));

    assertThat(VersionParser.parse("parquet-mr version 1.6.22rc99-SNAPSHOT (build abcd)"))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.6.22rc99-SNAPSHOT", "abcd"));

    assertThatThrownBy(() -> VersionParser.parse("unparseable string"))
        .isInstanceOf(VersionParseException.class)
        .hasMessage("Could not parse created_by: unparseable string using format: " + VersionParser.FORMAT);

    // missing semver
    assertThat(VersionParser.parse("parquet-mr version (build abcd)"))
        .isEqualTo(new ParsedVersion("parquet-mr", null, "abcd"));
    assertThat(VersionParser.parse("parquet-mr version  (build abcd)"))
        .isEqualTo(new ParsedVersion("parquet-mr", null, "abcd"));

    // missing build hash
    assertThat(VersionParser.parse("parquet-mr version 1.6.0 (build )"))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.6.0", null));
    assertThat(VersionParser.parse("parquet-mr version 1.6.0 (build)"))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.6.0", null));
    assertThat(VersionParser.parse("parquet-mr version (build)"))
        .isEqualTo(new ParsedVersion("parquet-mr", null, null));
    assertThat(VersionParser.parse("parquet-mr version (build )"))
        .isEqualTo(new ParsedVersion("parquet-mr", null, null));

    // Missing entire build section
    assertThat(VersionParser.parse("parquet-mr version 1.6.0"))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.6.0", null));
    assertThat(VersionParser.parse("parquet-mr version 1.8.0rc4"))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.8.0rc4", null));
    assertThat(VersionParser.parse("parquet-mr version 1.8.0rc4-SNAPSHOT"))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.8.0rc4-SNAPSHOT", null));
    assertThat(VersionParser.parse("parquet-mr version")).isEqualTo(new ParsedVersion("parquet-mr", null, null));

    // Various spaces
    assertThat(VersionParser.parse("parquet-mr     version    1.6.0"))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.6.0", null));
    assertThat(VersionParser.parse("parquet-mr     version    1.8.0rc4"))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.8.0rc4", null));
    assertThat(VersionParser.parse("parquet-mr      version    1.8.0rc4-SNAPSHOT  "))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.8.0rc4-SNAPSHOT", null));
    assertThat(VersionParser.parse("parquet-mr      version"))
        .isEqualTo(new ParsedVersion("parquet-mr", null, null));
    assertThat(VersionParser.parse("parquet-mr version 1.6.0 (  build )"))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.6.0", null));
    assertThat(VersionParser.parse("parquet-mr     version 1.6.0 (    build)"))
        .isEqualTo(new ParsedVersion("parquet-mr", "1.6.0", null));
    assertThat(VersionParser.parse("parquet-mr     version (    build)"))
        .isEqualTo(new ParsedVersion("parquet-mr", null, null));
    assertThat(VersionParser.parse("parquet-mr    version    (build    )"))
        .isEqualTo(new ParsedVersion("parquet-mr", null, null));
  }
}
