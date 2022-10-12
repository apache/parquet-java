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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class TestInteropReadLz4RawCodec {

  private static String SIMPLE_FILE = "/lz4_raw_compressed.parquet";
  private static String LARGER_FILE = "/lz4_raw_compressed_larger.parquet";

  @Test
  public void testInteropReadSimpleLz4RawParquetFile() throws IOException {
    // Test simple parquet file with lz4 raw compressed
    Path simpleFile = createPathFromCP(SIMPLE_FILE);
    readParquetFile(simpleFile, 4);
  }

  @Test
  public void testInteropReadLargerLz4RawParquetFile() throws IOException {
    // Test larger parquet file with lz4 raw compressed
    Path largerFile = createPathFromCP(LARGER_FILE);
    readParquetFile(largerFile, 10000);
  }

  private static Path createPathFromCP(String path) {
    try {
      return new Path(TestInteropReadLz4RawCodec.class.getResource(path).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private void readParquetFile(Path filePath, int expectedNumRows) throws IOException {
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), filePath).build()) {
      int numRows = 0;
      while (reader.read() != null) {
        numRows++;
      }
      reader.close();
      assertEquals(expectedNumRows, numRows);
    }
  }

}
