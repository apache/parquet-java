package org.apache.parquet;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
public class CorruptStatisticsTest {
  @Test
  public void testCorruptStatistics() {
    assertTrue(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.6.0 (build abcd)"));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.4.2 (build abcd)"));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.6.100 (build abcd)"));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.7.999 (build abcd)"));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.6.22rc99 (build abcd)"));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.6.22rc99-SNAPSHOT (build abcd)"));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.6.1-SNAPSHOT (build abcd)"));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics("unparseable string"));
    assertTrue(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.6.0t-01-abcdefg (build abcd)"));

    assertFalse(CorruptStatistics.shouldIgnoreStatistics("imapla version 1.6.0 (build abcd)"));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("imapla version 1.10.0 (build abcd)"));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.8.0 (build abcd)"));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.8.1 (build abcd)"));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.8.1rc3 (build abcd)"));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.8.1rc3-SNAPSHOT (build abcd)"));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.9.0 (build abcd)"));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 2.0.0 (build abcd)"));
    assertFalse(CorruptStatistics.shouldIgnoreStatistics("parquet-mr version 1.9.0t-01-abcdefg (build abcd)"));
  }
}
