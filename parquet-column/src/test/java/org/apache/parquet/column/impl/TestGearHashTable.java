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
package org.apache.parquet.column.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import org.apache.parquet.internal.column.chunking.RollingHashMask;
import org.junit.jupiter.api.Test;

/**
 * Pins {@link GearHashTable} to the specification Arrow C++ and arrow-rs generate their identical
 * tables from, so a transcription slip fails here instead of silently changing every boundary.
 */
public class TestGearHashTable {

  @Test
  public void tableMatchesTheMd5Specification() throws NoSuchAlgorithmException {
    // for seed in 0..7, n in 0..255: md5(bytes([seed] * 64 + [n] * 64)).hexdigest()[:16]
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    long[][] expected = new long[8][256];
    for (int seed = 0; seed < 8; ++seed) {
      for (int n = 0; n < 256; ++n) {
        byte[] input = new byte[128];
        Arrays.fill(input, 0, 64, (byte) seed);
        Arrays.fill(input, 64, 128, (byte) n);
        byte[] digest = md5.digest(input);
        // The first 16 hex characters of the digest are its first 8 bytes, big-endian.
        expected[seed][n] = ByteBuffer.wrap(digest).getLong();
      }
    }

    assertThat(GearHashTable.TABLE).isDeepEqualTo(expected);
  }

  /**
   * The mask arithmetic divides the per-match target by the number of tables, so the two have to
   * agree; they are declared apart because the count belongs with the mask and the tables cannot
   * be reached from there.
   */
  @Test
  public void thereAreAsManyTablesAsAMatchRunNeeds() {
    assertThat(GearHashTable.TABLE.length).isEqualTo(RollingHashMask.NUM_GEARHASH_TABLES);
  }
}
