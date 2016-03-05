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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Arrays;

import static java.util.UUID.randomUUID;
import static org.apache.parquet.benchmarks.BenchmarkUtils.deleteIfExists;
import static org.apache.parquet.benchmarks.BenchmarkUtils.exists;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.benchmarks.BenchmarkConstants.*;
import static org.apache.parquet.benchmarks.BenchmarkFiles.*;

public class DataGenerator {

  public void generateAll() {
    try {
      generateData(file_1M, defaultConfiguration, PARQUET_1_0, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, UNCOMPRESSED, ONE_MILLION);

      //generate data for different block and page sizes
      generateData(file_1M_BS256M_PS4M, defaultConfiguration, PARQUET_1_0, BLOCK_SIZE_256M, PAGE_SIZE_4M, UNCOMPRESSED, ONE_MILLION);
      generateData(file_1M_BS256M_PS8M, defaultConfiguration, PARQUET_1_0, BLOCK_SIZE_256M, PAGE_SIZE_8M, UNCOMPRESSED, ONE_MILLION);
      generateData(file_1M_BS512M_PS4M, defaultConfiguration, PARQUET_1_0, BLOCK_SIZE_512M, PAGE_SIZE_4M, UNCOMPRESSED, ONE_MILLION);
      generateData(file_1M_BS512M_PS8M, defaultConfiguration, PARQUET_1_0, BLOCK_SIZE_512M, PAGE_SIZE_8M, UNCOMPRESSED, ONE_MILLION);

      //generate data for different codecs
//      generateData(parquetFile_1M_LZO, defaultConfiguration, PARQUET_1_0, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, LZO, ONE_MILLION);

      generateData(file_1M_SNAPPY, defaultConfiguration, PARQUET_1_0, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, SNAPPY, ONE_MILLION);
      generateData(file_1M_GZIP, defaultConfiguration, PARQUET_1_0, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, GZIP, ONE_MILLION);
      generateData(file_10M_GZIP, defaultConfiguration, PARQUET_1_0, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, GZIP, TEN_MILLION);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void generateData(Path outFile, Configuration configuration, ParquetProperties.WriterVersion version,
                           int blockSize, int pageSize, CompressionCodecName codec, int nRows)
          throws IOException
  {
    if (exists(configuration, outFile)) {
      System.out.println("File already exists " + outFile);
      return;
    }

    System.out.println("Generating data @ " + outFile);

    MessageType schema = parseMessageType(
            "message test { "
                    + "required binary binary_field; "
                    + "required int32 int32_field; "
                    + "required int64 int64_field; "
                    + "required boolean boolean_field; "
                    + "required float float_field; "
                    + "required double double_field; "
                    + "required fixed_len_byte_array(" + FIXED_LEN_BYTEARRAY_SIZE +") flba_field; "
                    + "required int96 int96_field; "
                    + "} ");

    GroupWriteSupport.setSchema(schema, configuration);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = new ParquetWriter<Group>(outFile, new GroupWriteSupport(), codec, blockSize,
                                                           pageSize, DICT_PAGE_SIZE, true, false, version, configuration);

    //generate some data for the fixed len byte array field
    char[] chars = new char[FIXED_LEN_BYTEARRAY_SIZE];
    Arrays.fill(chars, '*');

    for (int i = 0; i < nRows; i++) {
      writer.write(
        f.newGroup()
          .append("binary_field", randomUUID().toString())
          .append("int32_field", i)
          .append("int64_field", 64l)
          .append("boolean_field", true)
          .append("float_field", 1.0f)
          .append("double_field", 2.0d)
          .append("flba_field", new String(chars))
          .append("int96_field", Binary.fromConstantByteArray(new byte[12]))
      );
    }
    writer.close();
  }

  public void cleanup()
  {
    deleteIfExists(defaultConfiguration, file_1M);
    deleteIfExists(defaultConfiguration, file_1M_BS256M_PS4M);
    deleteIfExists(defaultConfiguration, file_1M_BS256M_PS8M);
    deleteIfExists(defaultConfiguration, file_1M_BS512M_PS4M);
    deleteIfExists(defaultConfiguration, file_1M_BS512M_PS8M);
//    deleteIfExists(defaultConfiguration, parquetFile_1M_LZO);
    deleteIfExists(defaultConfiguration, file_1M_SNAPPY);
    deleteIfExists(defaultConfiguration, file_1M_GZIP);
    deleteIfExists(defaultConfiguration, file_10M_GZIP);
  }

  public static void main(String[] args) {
    DataGenerator generator = new DataGenerator();
    if (args.length < 1) {
      System.err.println("Please specify a command (generate|cleanup).");
      System.exit(1);
    }

    String command = args[0];
    if (command.equalsIgnoreCase("generate")) {
      generator.generateAll();
    } else if (command.equalsIgnoreCase("cleanup")) {
      generator.cleanup();
    } else {
      throw new IllegalArgumentException("invalid command " + command);
    }
  }
}
