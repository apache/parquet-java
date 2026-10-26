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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestAdaptiveBlockSplitBloomFiltering {

  private final TestBloomFiltering bloomTests = new TestBloomFiltering();

  @BeforeAll
  public static void createFiles() throws IOException {
    TestBloomFiltering.createFiles(true);
  }

  @AfterAll
  public static void deleteFiles() throws IOException {
    TestBloomFiltering.deleteFiles();
  }

  static Stream<Arguments> fileAndEncryption() {
    return TestBloomFiltering.fileAndEncryption();
  }

  @ParameterizedTest
  @MethodSource("fileAndEncryption")
  public void testSimpleFiltering(Path file, boolean isEncrypted) throws IOException {
    bloomTests.testSimpleFiltering(file, isEncrypted);
  }

  @ParameterizedTest
  @MethodSource("fileAndEncryption")
  public void testNestedFiltering(Path file, boolean isEncrypted) throws IOException {
    bloomTests.testNestedFiltering(file, isEncrypted);
  }

  @ParameterizedTest
  @MethodSource("fileAndEncryption")
  public void checkBloomFilterSize(Path file, boolean isEncrypted) throws IOException {
    FileDecryptionProperties fileDecryptionProperties = bloomTests.getFileDecryptionProperties(isEncrypted);
    final ParquetReadOptions readOptions = ParquetReadOptions.builder()
        .withDecryption(fileDecryptionProperties)
        .build();
    InputFile inputFile = HadoopInputFile.fromPath(file, new Configuration());
    try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile, readOptions)) {
      fileReader.getRowGroups().forEach(block -> {
        BloomFilterReader bloomFilterReader = fileReader.getBloomFilterDataReader(block);
        block.getColumns().stream()
            .filter(column -> column.getBloomFilterOffset() > 0)
            .forEach(column -> {
              int bitsetSize =
                  bloomFilterReader.readBloomFilter(column).getBitsetSize();
              // set 10 candidates:
              // [byteSize=2048, expectedNVD=1500], [byteSize=4096, expectedNVD=3000], [byteSize=6500,
              // expectedNVD=8192],
              // [byteSize=16384, expectedNVD=13500], [byteSize=32768, expectedNVD=27000] ......
              // number of distinct values is less than 100, so the byteSize should be less than 2048.
              assertThat(bitsetSize).isLessThanOrEqualTo(2048);
            });
      });
    }
  }
}
