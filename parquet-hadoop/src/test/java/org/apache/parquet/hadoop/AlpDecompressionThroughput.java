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
package org.apache.parquet.hadoop;

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Test;

/**
 * Measures ALP decompression throughput in bytes/second for real-world datasets.
 * Reports raw (uncompressed) bytes/second and compressed bytes/second.
 */
public class AlpDecompressionThroughput {

  private static final int WARMUP_ITERS = 5;
  private static final int MEASURED_ITERS = 20;

  private static Path resourcePath(String name) {
    try {
      return new Path(AlpDecompressionThroughput.class.getResource("/" + name).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void measureDecompressionThroughput() throws IOException {
    System.out.println();
    System.out.println("=== ALP Decompression Throughput ===");
    System.out.printf(
        "%-40s %8s %6s %6s %12s %12s %12s%n",
        "File", "Rows", "Cols", "Type", "Compressed", "Raw MB/s", "Comp MB/s");
    System.out.println(
        "---------------------------------------- -------- ------ ------ ------------ ------------ ------------");

    // Double datasets
    benchmarkFile("alp_arade.parquet", 15000, 4, "double", 8);
    benchmarkFile("alp_spotify1.parquet", 15000, 9, "double", 8);
    benchmarkFile("alp_java_arade.parquet", 15000, 4, "double", 8);
    benchmarkFile("alp_java_spotify1.parquet", 15000, 9, "double", 8);

    // Float datasets
    benchmarkFile("alp_float_arade.parquet", 15000, 4, "float", 4);
    benchmarkFile("alp_float_spotify1.parquet", 15000, 9, "float", 4);
    benchmarkFile("alp_java_float_arade.parquet", 15000, 4, "float", 4);
    benchmarkFile("alp_java_float_spotify1.parquet", 15000, 9, "float", 4);

    System.out.println();
  }

  private void benchmarkFile(String fileName, int expectedRows, int numCols, String type, int bytesPerValue)
      throws IOException {
    Path path = resourcePath(fileName);

    // Get compressed file size from parquet metadata
    long compressedSize = 0;
    try (ParquetFileReader pfr = ParquetFileReader.open(
        org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
            path, new org.apache.hadoop.conf.Configuration()))) {
      ParquetMetadata footer = pfr.getFooter();
      for (org.apache.parquet.hadoop.metadata.BlockMetaData block : footer.getBlocks()) {
        compressedSize += block.getTotalByteSize();
      }
    }

    long rawBytes = (long) expectedRows * numCols * bytesPerValue;

    // Warmup
    for (int i = 0; i < WARMUP_ITERS; i++) {
      readAllValues(path, type, numCols);
    }

    // Measured runs
    long totalNanos = 0;
    for (int i = 0; i < MEASURED_ITERS; i++) {
      long start = System.nanoTime();
      readAllValues(path, type, numCols);
      totalNanos += System.nanoTime() - start;
    }

    double avgSeconds = (totalNanos / (double) MEASURED_ITERS) / 1_000_000_000.0;
    double rawMBps = (rawBytes / avgSeconds) / (1024.0 * 1024.0);
    double compMBps = (compressedSize / avgSeconds) / (1024.0 * 1024.0);

    System.out.printf(
        "%-40s %8d %6d %6s %12d %10.1f %10.1f%n",
        fileName, expectedRows, numCols, type, compressedSize, rawMBps, compMBps);
  }

  private void readAllValues(Path path, String type, int numCols) throws IOException {
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), path).build()) {
      Group group;
      if ("double".equals(type)) {
        while ((group = reader.read()) != null) {
          for (int c = 0; c < numCols; c++) {
            group.getDouble(c, 0);
          }
        }
      } else {
        while ((group = reader.read()) != null) {
          for (int c = 0; c < numCols; c++) {
            group.getFloat(c, 0);
          }
        }
      }
    }
  }
}
