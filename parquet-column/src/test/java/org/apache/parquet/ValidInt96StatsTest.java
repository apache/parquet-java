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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ValidInt96StatsTest {

  @Test
  public void testNullAndEmpty() {
    assertFalse(ValidInt96Stats.hasValidInt96Stats(null));
    assertFalse(ValidInt96Stats.hasValidInt96Stats(""));
  }

  @Test
  public void testParquetMrValid() {
    // Versions > 1.15.0 should be valid
    assertTrue(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 1.16.0"));
    assertTrue(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 1.15.1"));
    assertTrue(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 2.0.0"));
    assertTrue(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 1.16.0 (build abcd)"));
    assertTrue(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 1.15.1-SNAPSHOT"));
  }

  @Test
  public void testParquetMrInvalid() {
    // Versions <= 1.15.0 should be invalid
    assertFalse(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 1.15.0"));
    assertFalse(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 1.12.3"));
    assertFalse(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 1.14.0"));
    assertFalse(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 1.12.3 (build abcd)"));
    assertFalse(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 1.12.3-SNAPSHOT"));
    assertFalse(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 1.12.3rc1"));
    assertFalse(ValidInt96Stats.hasValidInt96Stats("parquet-mr version 1.12.3rc1-SNAPSHOT"));
  }

  @Test
  public void testParquetMrCompatiblePhotonValid() {
    assertTrue(ValidInt96Stats.hasValidInt96Stats("parquet-mr compatible Photon version 1.0.0"));
    assertTrue(ValidInt96Stats.hasValidInt96Stats("parquet-mr compatible Photon version 1.0.0 (build abcd)"));
    assertTrue(ValidInt96Stats.hasValidInt96Stats("parquet-mr compatible Photon version 1.0.0-SNAPSHOT"));
    assertTrue(ValidInt96Stats.hasValidInt96Stats("parquet-mr compatible Photon version 1.0.0rc1"));
    assertTrue(ValidInt96Stats.hasValidInt96Stats("parquet-mr compatible Photon version 1.0.0rc1-SNAPSHOT"));
  }

  @Test
  public void testInvalidApplications() {
    assertFalse(ValidInt96Stats.hasValidInt96Stats("arrow-rs version 0.1.0"));
    assertFalse(ValidInt96Stats.hasValidInt96Stats("impala version 1.6.0"));
  }
}
