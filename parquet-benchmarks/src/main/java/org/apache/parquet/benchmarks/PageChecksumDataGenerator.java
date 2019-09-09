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
package org.apache.parquet.benchmarks;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import static java.util.UUID.randomUUID;
import static org.apache.parquet.benchmarks.BenchmarkConstants.*;
import static org.apache.parquet.benchmarks.BenchmarkFiles.*;

import java.io.IOException;
import java.util.Random;

import static org.apache.parquet.benchmarks.BenchmarkUtils.deleteIfExists;
import static org.apache.parquet.benchmarks.BenchmarkUtils.exists;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.*;

public class PageChecksumDataGenerator {

  private final MessageType SCHEMA = MessageTypeParser.parseMessageType(
    "message m {" +
      "  required int64 long_field;" +
      "  required binary binary_field;" +
      "  required group group {" +
      "    repeated int32 int_field;" +
      "  }" +
      "}");

  public void generateData(Path outFile, int nRows, boolean writeChecksums,
                           CompressionCodecName compression) throws IOException {
    if (exists(configuration, outFile)) {
      System.out.println("File already exists " + outFile);
      return;
    }

    ParquetWriter<Group> writer = ExampleParquetWriter.builder(outFile)
      .withConf(configuration)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withCompressionCodec(compression)
      .withDictionaryEncoding(true)
      .withType(SCHEMA)
      .withPageWriteChecksumEnabled(writeChecksums)
      .build();

    GroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);
    Random rand = new Random(42);
    for (int i = 0; i < nRows; i++) {
      Group group = groupFactory.newGroup();
      group
        .append("long_field", (long) i)
        .append("binary_field", randomUUID().toString())
        .addGroup("group")
        // Force dictionary encoding by performing modulo
        .append("int_field", rand.nextInt() % 100)
        .append("int_field", rand.nextInt() % 100)
        .append("int_field", rand.nextInt() % 100)
        .append("int_field", rand.nextInt() % 100);
      writer.write(group);
    }

    writer.close();
  }

  public void generateAll() {
    try {
      // No need to generate the non-checksum versions, as the files generated here are only used in
      // the read benchmarks
      generateData(file_100K_CHECKSUMS_UNCOMPRESSED, 100 * ONE_K, true, UNCOMPRESSED);
      generateData(file_100K_CHECKSUMS_GZIP, 100 * ONE_K, true, GZIP);
      generateData(file_100K_CHECKSUMS_SNAPPY, 100 * ONE_K, true, SNAPPY);
      generateData(file_1M_CHECKSUMS_UNCOMPRESSED, ONE_MILLION, true, UNCOMPRESSED);
      generateData(file_1M_CHECKSUMS_GZIP, ONE_MILLION, true, GZIP);
      generateData(file_1M_CHECKSUMS_SNAPPY, ONE_MILLION, true, SNAPPY);
      generateData(file_10M_CHECKSUMS_UNCOMPRESSED, 10 * ONE_MILLION, true, UNCOMPRESSED);
      generateData(file_10M_CHECKSUMS_GZIP, 10 * ONE_MILLION, true, GZIP);
      generateData(file_10M_CHECKSUMS_SNAPPY, 10 * ONE_MILLION, true, SNAPPY);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void cleanup() {
    deleteIfExists(configuration, file_100K_NOCHECKSUMS_UNCOMPRESSED);
    deleteIfExists(configuration, file_100K_CHECKSUMS_UNCOMPRESSED);
    deleteIfExists(configuration, file_100K_NOCHECKSUMS_GZIP);
    deleteIfExists(configuration, file_100K_CHECKSUMS_GZIP);
    deleteIfExists(configuration, file_100K_NOCHECKSUMS_SNAPPY);
    deleteIfExists(configuration, file_100K_CHECKSUMS_SNAPPY);
    deleteIfExists(configuration, file_1M_NOCHECKSUMS_UNCOMPRESSED);
    deleteIfExists(configuration, file_1M_CHECKSUMS_UNCOMPRESSED);
    deleteIfExists(configuration, file_1M_NOCHECKSUMS_GZIP);
    deleteIfExists(configuration, file_1M_CHECKSUMS_GZIP);
    deleteIfExists(configuration, file_1M_NOCHECKSUMS_SNAPPY);
    deleteIfExists(configuration, file_1M_CHECKSUMS_SNAPPY);
    deleteIfExists(configuration, file_10M_NOCHECKSUMS_UNCOMPRESSED);
    deleteIfExists(configuration, file_10M_CHECKSUMS_UNCOMPRESSED);
    deleteIfExists(configuration, file_10M_NOCHECKSUMS_GZIP);
    deleteIfExists(configuration, file_10M_CHECKSUMS_GZIP);
    deleteIfExists(configuration, file_10M_NOCHECKSUMS_SNAPPY);
    deleteIfExists(configuration, file_10M_CHECKSUMS_SNAPPY);
  }
}
