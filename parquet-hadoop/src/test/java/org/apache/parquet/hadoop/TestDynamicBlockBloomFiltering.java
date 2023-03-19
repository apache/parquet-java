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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

public class TestDynamicBlockBloomFiltering extends TestBloomFiltering {

  @BeforeClass
  public static void createFiles() throws IOException {
    createFiles(true);
  }

  public TestDynamicBlockBloomFiltering(Path file, boolean isEncrypted) {
    super(file, isEncrypted);
  }

  @Test
  public void testSimpleFiltering() throws IOException {
    super.testSimpleFiltering();
  }

  @Test
  public void testNestedFiltering() throws IOException {
    super.testNestedFiltering();
  }

  @Test
  public void checkBloomFilterSize() throws IOException {
    FileDecryptionProperties fileDecryptionProperties = getFileDecryptionProperties();
    final ParquetReadOptions readOptions = ParquetReadOptions.builder().withDecryption(fileDecryptionProperties).build();
    InputFile inputFile = HadoopInputFile.fromPath(getFile(), new Configuration());
    try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile, readOptions)) {
      fileReader.getRowGroups().forEach(block -> {
        BloomFilterReader bloomFilterReader = fileReader.getBloomFilterDataReader(block);
        block.getColumns().stream()
          .filter(column -> column.getBloomFilterOffset() > 0)
          .forEach(column -> {
            int bitsetSize = bloomFilterReader.readBloomFilter(column).getBitsetSize();
            // set 10 candidates:
            // [bitSize=2048, expectedNVD=1500], [bitSize=4096, expectedNVD=3000], [bitSize=6500, expectedNVD=8192],
            // [bitSize=16384, expectedNVD=1350], [bitSize=32768, expectedNVD=27000] ......
            // number of distinct values is less than 100, so the bitSize should be less than 2048.
            assertTrue(bitsetSize <= 2048);
          });
      });
    }
  }
}
