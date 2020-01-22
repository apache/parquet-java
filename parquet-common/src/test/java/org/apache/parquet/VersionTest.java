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

import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.VersionParser.VersionParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This test doesn't do much, but it makes sure that the Version class was
 * properly generated, and that it's VERSION_NUMBER field has been populated
 * correctly. The hope is to catch any issues like the version being an empty
 * string or something along those lines.
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
    assertEquals(Version.VERSION_NUMBER, version.version);
    assertEquals("parquet-mr", version.application);
  }

  @Test
  public void testVersionParser() throws Exception {
    assertEquals(new ParsedVersion("parquet-mr", "1.6.0", "abcd"),
        VersionParser.parse("parquet-mr version 1.6.0 (build abcd)"));

    assertEquals(new ParsedVersion("parquet-mr", "1.6.22rc99-SNAPSHOT", "abcd"),
        VersionParser.parse("parquet-mr version 1.6.22rc99-SNAPSHOT (build abcd)"));

    try {
      VersionParser.parse("unparseable string");
      fail("this should throw");
    } catch (VersionParseException e) {
      //
    }

    // missing semver
    assertEquals(new ParsedVersion("parquet-mr", null, "abcd"), VersionParser.parse("parquet-mr version (build abcd)"));
    assertEquals(new ParsedVersion("parquet-mr", null, "abcd"),
        VersionParser.parse("parquet-mr version  (build abcd)"));

    // missing build hash
    assertEquals(new ParsedVersion("parquet-mr", "1.6.0", null),
        VersionParser.parse("parquet-mr version 1.6.0 (build )"));
    assertEquals(new ParsedVersion("parquet-mr", "1.6.0", null),
        VersionParser.parse("parquet-mr version 1.6.0 (build)"));
    assertEquals(new ParsedVersion("parquet-mr", null, null), VersionParser.parse("parquet-mr version (build)"));
    assertEquals(new ParsedVersion("parquet-mr", null, null), VersionParser.parse("parquet-mr version (build )"));

    // Missing entire build section
    assertEquals(new ParsedVersion("parquet-mr", "1.6.0", null), VersionParser.parse("parquet-mr version 1.6.0"));
    assertEquals(new ParsedVersion("parquet-mr", "1.8.0rc4", null), VersionParser.parse("parquet-mr version 1.8.0rc4"));
    assertEquals(new ParsedVersion("parquet-mr", "1.8.0rc4-SNAPSHOT", null),
        VersionParser.parse("parquet-mr version 1.8.0rc4-SNAPSHOT"));
    assertEquals(new ParsedVersion("parquet-mr", null, null), VersionParser.parse("parquet-mr version"));

    // Various spaces
    assertEquals(new ParsedVersion("parquet-mr", "1.6.0", null),
        VersionParser.parse("parquet-mr     version    1.6.0"));
    assertEquals(new ParsedVersion("parquet-mr", "1.8.0rc4", null),
        VersionParser.parse("parquet-mr     version    1.8.0rc4"));
    assertEquals(new ParsedVersion("parquet-mr", "1.8.0rc4-SNAPSHOT", null),
        VersionParser.parse("parquet-mr      version    1.8.0rc4-SNAPSHOT  "));
    assertEquals(new ParsedVersion("parquet-mr", null, null), VersionParser.parse("parquet-mr      version"));
    assertEquals(new ParsedVersion("parquet-mr", "1.6.0", null),
        VersionParser.parse("parquet-mr version 1.6.0 (  build )"));
    assertEquals(new ParsedVersion("parquet-mr", "1.6.0", null),
        VersionParser.parse("parquet-mr     version 1.6.0 (    build)"));
    assertEquals(new ParsedVersion("parquet-mr", null, null),
        VersionParser.parse("parquet-mr     version (    build)"));
    assertEquals(new ParsedVersion("parquet-mr", null, null),
        VersionParser.parse("parquet-mr    version    (build    )"));
  }
}
