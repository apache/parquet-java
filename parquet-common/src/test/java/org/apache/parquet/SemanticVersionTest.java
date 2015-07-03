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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
  public void testParse() throws Exception {
    assertEquals(new SemanticVersion(1, 8, 0), SemanticVersion.parse("1.8.0"));
    assertEquals(new SemanticVersion(1, 8, 0, true), SemanticVersion.parse("1.8.0rc3"));
    assertEquals(new SemanticVersion(1, 8, 0, true), SemanticVersion.parse("1.8.0rc3-SNAPSHOT"));
    assertEquals(new SemanticVersion(1, 8, 0, true), SemanticVersion.parse("1.8.0-SNAPSHOT"));
  }
}
