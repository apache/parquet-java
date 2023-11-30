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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class SemanticVersionTest {
  @Test
  public void testCompare() {
    assertTrue(new SemanticVersion(1, 8, 1).compareTo(new SemanticVersion(1, 8, 1)) == 0);
    assertTrue(new SemanticVersion(1, 8, 0).compareTo(new SemanticVersion(1, 8, 1)) < 0);
    assertTrue(new SemanticVersion(1, 8, 2).compareTo(new SemanticVersion(1, 8, 1)) > 0);

    assertTrue(new SemanticVersion(1, 8, 1).compareTo(new SemanticVersion(1, 8, 1)) == 0);
    assertTrue(new SemanticVersion(1, 8, 0).compareTo(new SemanticVersion(1, 8, 1)) < 0);
    assertTrue(new SemanticVersion(1, 8, 2).compareTo(new SemanticVersion(1, 8, 1)) > 0);

    assertTrue(new SemanticVersion(1, 7, 0).compareTo(new SemanticVersion(1, 8, 0)) < 0);
    assertTrue(new SemanticVersion(1, 9, 0).compareTo(new SemanticVersion(1, 8, 0)) > 0);

    assertTrue(new SemanticVersion(0, 0, 0).compareTo(new SemanticVersion(1, 0, 0)) < 0);
    assertTrue(new SemanticVersion(2, 0, 0).compareTo(new SemanticVersion(1, 0, 0)) > 0);

    assertTrue(new SemanticVersion(1, 8, 100).compareTo(new SemanticVersion(1, 9, 0)) < 0);

    assertTrue(new SemanticVersion(1, 8, 0).compareTo(new SemanticVersion(1, 8, 0, true)) > 0);
    assertTrue(new SemanticVersion(1, 8, 0, true).compareTo(new SemanticVersion(1, 8, 0, true)) == 0);
    assertTrue(new SemanticVersion(1, 8, 0, true).compareTo(new SemanticVersion(1, 8, 0)) < 0);
  }

  @Test
  public void testSemverPrereleaseExamples() throws Exception {
    List<String> examples = Arrays.asList(
        "1.0.0-alpha",
        "1.0.0-alpha.1",
        "1.0.0-alpha.beta",
        "1.0.0-beta",
        "1.0.0-beta.2",
        "1.0.0-beta.11",
        "1.0.0-rc.1",
        "1.0.0");
    for (int i = 0; i < examples.size() - 1; i += 1) {
      assertLessThan(examples.get(i), examples.get(i + 1));
      assertEqualTo(examples.get(i), examples.get(i));
    }
    // the last one didn't get reflexively tested
    assertEqualTo(examples.get(examples.size() - 1), examples.get(examples.size() - 1));
  }

  @Test
  public void testSemverBuildInfoExamples() throws Exception {
    assertEqualTo("1.0.0-alpha+001", "1.0.0-alpha+001");
    assertEqualTo("1.0.0-alpha", "1.0.0-alpha+001");
    assertEqualTo("1.0.0+20130313144700", "1.0.0+20130313144700");
    assertEqualTo("1.0.0", "1.0.0+20130313144700");
    assertEqualTo("1.0.0-beta+exp.sha.5114f85", "1.0.0-beta+exp.sha.5114f85");
    assertEqualTo("1.0.0-beta", "1.0.0-beta+exp.sha.5114f85");
  }

  @Test
  public void testUnknownComparisons() throws Exception {
    // anything with unknown is lower precedence
    assertLessThan("1.0.0rc0-alpha+001", "1.0.0-alpha");
  }

  @Test
  public void testDistributionVersions() throws Exception {
    assertEqualTo("1.5.0-cdh5.5.0", "1.5.0-cdh5.5.0");
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0-cdh5.5.1");
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0-cdh5.5.1-SNAPSHOT");
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0-cdh5.6.0");
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0-cdh6.0.0");
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0");
    // according to the semver spec, this is true :(
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0-cdh5.5.0-SNAPSHOT");
  }

  @Test
  public void testParse() throws Exception {
    assertEquals(new SemanticVersion(1, 8, 0), SemanticVersion.parse("1.8.0"));
    assertEquals(new SemanticVersion(1, 8, 0, true), SemanticVersion.parse("1.8.0rc3"));
    assertEquals(new SemanticVersion(1, 8, 0, "rc3", "SNAPSHOT", null), SemanticVersion.parse("1.8.0rc3-SNAPSHOT"));
    assertEquals(new SemanticVersion(1, 8, 0, null, "SNAPSHOT", null), SemanticVersion.parse("1.8.0-SNAPSHOT"));
    assertEquals(new SemanticVersion(1, 5, 0, null, "cdh5.5.0", null), SemanticVersion.parse("1.5.0-cdh5.5.0"));
  }

  private static void assertLessThan(String a, String b) throws SemanticVersion.SemanticVersionParseException {
    assertTrue(a + " should be < " + b, SemanticVersion.parse(a).compareTo(SemanticVersion.parse(b)) < 0);
    assertTrue(b + " should be > " + a, SemanticVersion.parse(b).compareTo(SemanticVersion.parse(a)) > 0);
  }

  private static void assertEqualTo(String a, String b) throws SemanticVersion.SemanticVersionParseException {
    assertTrue(a + " should equal " + b, SemanticVersion.parse(a).compareTo(SemanticVersion.parse(b)) == 0);
  }
}
