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

package org.apache.parquet.hadoop.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.InterOpTester;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.Test;

public class TestInteropReadLz4RawCodec {
  private static final String CHANGESET = "19fcd4d";
  private static String SIMPLE_FILE = "lz4_raw_compressed.parquet";
  private static String LARGER_FILE = "lz4_raw_compressed_larger.parquet";

  private InterOpTester interop = new InterOpTester();

  @Test
  public void testInteropReadLz4RawSimpleParquetFiles() throws IOException {
    // Test simple parquet file with lz4 raw compressed
    Path simpleFile = interop.GetInterOpFile(SIMPLE_FILE, CHANGESET);
    final int expectRows = 4;
    long[] c0ExpectValues = {1593604800, 1593604800, 1593604801, 1593604801};
    String[] c1ExpectValues = {"abc", "def", "abc", "def"};
    double[] c2ExpectValues = {42.0, 7.7, 42.125, 7.7};

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), simpleFile).build()) {
      for (int i = 0; i < expectRows; ++i) {
        Group group = reader.read();
        assertTrue(group != null);
        assertEquals(c0ExpectValues[i], group.getLong(0, 0));
        assertEquals(c1ExpectValues[i], group.getString(1, 0));
        assertEquals(c2ExpectValues[i], group.getDouble(2, 0), 0.000001);
      }
      assertTrue(reader.read() == null);
    }
  }

  @Test
  public void testInteropReadLz4RawLargerParquetFiles() throws IOException {
    // Test larger parquet file with lz4 raw compressed
    final int expectRows = 10000;
    Path largerFile = interop.GetInterOpFile(LARGER_FILE, CHANGESET);
    String[] c0ExpectValues = {
      "c7ce6bef-d5b0-4863-b199-8ea8c7fb117b",
      "e8fb9197-cb9f-4118-b67f-fbfa65f61843",
      "ab52a0cc-c6bb-4d61-8a8f-166dc4b8b13c",
      "85440778-460a-41ac-aa2e-ac3ee41696bf"
    };

    int index = 0;
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), largerFile).build()) {
      for (int i = 0; i < expectRows; ++i) {
        Group group = reader.read();
        assertTrue(group != null);
        if (i == 0 || i == 1 || i == expectRows - 2 || i == expectRows - 1) {
          assertEquals(c0ExpectValues[index], group.getString(0, 0));
          index++;
        }
      }
      assertTrue(reader.read() == null);
    }
  }
}
