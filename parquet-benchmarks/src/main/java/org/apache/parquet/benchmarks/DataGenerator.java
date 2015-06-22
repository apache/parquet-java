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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.util.UUID.randomUUID;
import static org.apache.parquet.benchmarks.BenchmarkUtils.deleteIfExists;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.benchmarks.BenchmarkConstants.*;
import static org.apache.parquet.benchmarks.BenchmarkFiles.*;

public class DataGenerator {
  public static final String DEFAULT_SCHEMA = "message simple_test { "
      + " required binary binary_field; "
      + " required int32 int32_field; "
      + " required int64 int64_field; "
      + " required boolean boolean_field; "
      + " required float float_field; "
      + " required double double_field; "
      + " required fixed_len_byte_array(100) flba_field; "
      + " required int96 int96_field; "
      + "} ";

  private MessageType schema;
  private Configuration configuration;
  private WriterVersion writerVersion;
  private SimpleGroupFactory groupFactory;
  private GroupWriteSupport groupWriteSupport;

  //use a constant seed to generate the same random data for all tests
  private Random rand = new Random(1);
  private List<Group> randomData;

  public DataGenerator(Configuration configuration) {
    this(configuration, WriterVersion.PARQUET_2_0);
  }

  public DataGenerator(Configuration configuration, WriterVersion writerVersion) {
    this.configuration = configuration;
    this.writerVersion = writerVersion;
    this.schema = parseMessageType(DEFAULT_SCHEMA);
    this.groupFactory = new SimpleGroupFactory(schema);

    GroupWriteSupport.setSchema(schema, configuration);
    this.groupWriteSupport = new GroupWriteSupport();

    randomData = new ArrayList<Group>();

    //generates just one random record by default
    initializeRandomSampleData(1);
  }

  public void setWriterVersion(WriterVersion writerVersion) {
    this.writerVersion = writerVersion;
  }

  public void initializeRandomSampleData(int sampleSize) {
    randomData.clear();
    for (int i = 0; i < sampleSize; i++) {
      randomData.add(randomRecord());
    }
  }

  public Group randomRecord() {
    byte[] int96 = new byte[12];
    rand.nextBytes(int96);

    return groupFactory.newGroup()
        .append("binary_field", randomUUID().toString())
        .append("int32_field", RandomUtils.nextInt())
        .append("int64_field", RandomUtils.nextLong())
        .append("boolean_field", RandomUtils.nextBoolean())
        .append("float_field", RandomUtils.nextFloat())
        .append("double_field", RandomUtils.nextDouble())
        .append("flba_field", RandomStringUtils.randomAlphanumeric(100))
        .append("int96_field", Binary.fromByteArray(int96));
  }

  public void generateAll() {
    try {
      generateData(file_1M, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, UNCOMPRESSED, ONE_MILLION);

      //generate data for different block and page sizes
      generateData(file_1M_BS256M_PS4M, BLOCK_SIZE_256M, PAGE_SIZE_4M, UNCOMPRESSED, ONE_MILLION);
      generateData(file_1M_BS256M_PS8M, BLOCK_SIZE_256M, PAGE_SIZE_8M, UNCOMPRESSED, ONE_MILLION);
      generateData(file_1M_BS512M_PS4M, BLOCK_SIZE_512M, PAGE_SIZE_4M, UNCOMPRESSED, ONE_MILLION);
      generateData(file_1M_BS512M_PS8M, BLOCK_SIZE_512M, PAGE_SIZE_8M, UNCOMPRESSED, ONE_MILLION);

      //generate data for different codecs
//      generateData(parquetFile_1M_LZO, configuration, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, LZO, ONE_MILLION);
      generateData(file_1M_SNAPPY, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, SNAPPY, ONE_MILLION);
      generateData(file_1M_GZIP, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, GZIP, ONE_MILLION);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void generateData(Path outFile, int blockSize, int pageSize, CompressionCodecName codec, int nRows)
          throws IOException
  {
    ParquetWriter<Group> writer = new ParquetWriter<Group>(outFile, groupWriteSupport, codec, blockSize,
                                                           pageSize, pageSize, true, false, writerVersion, configuration);

    for (int i = 0; i < nRows; i += randomData.size()) {
      for (int j = 0; j < randomData.size() && j < (nRows - i); j++) {
        writer.write(randomData.get(j));
      }
    }

    writer.close();
  }

  public void cleanup()
  {
    deleteIfExists(configuration, file_1M);
    deleteIfExists(configuration, file_1M_BS256M_PS4M);
    deleteIfExists(configuration, file_1M_BS256M_PS8M);
    deleteIfExists(configuration, file_1M_BS512M_PS4M);
    deleteIfExists(configuration, file_1M_BS512M_PS8M);
//    deleteIfExists(configuration, parquetFile_1M_LZO);
    deleteIfExists(configuration, file_1M_SNAPPY);
    deleteIfExists(configuration, file_1M_GZIP);
  }

  public static void main(String[] args) {
    DataGenerator generator = new DataGenerator(new Configuration());

    if (args.length < 1) {
      System.err.println("Please specify a command (generate VERSION|cleanup).");
      System.exit(1);
    }

    String command = args[0];
    if (command.equalsIgnoreCase("generate")) {
      if (args.length < 2 || args.length > 3) {
        System.err.println("Usage: generate VERSION [-randomData]\n");
        System.err.println("Options:");
        System.err.println("  VERSION         Use a specific parquet file version to generate data.");
        System.err.println("                  - v1 for PARQUET_1_0 version.");
        System.err.println("                  - v2 for PARQUET_2_0 version.");
        System.err.println("  -randomData     This flag specifies if random data will be used to generate data.");
        System.err.println("                  By default, only 1 random row is generated, and repeated across the file.");
        System.exit(1);
      }

      String parquetVersion = args[1];

      if (args.length == 3 && args[2].equalsIgnoreCase("-randomData")) {
        generator.initializeRandomSampleData(RANDOM_SAMPLE_ROWS);
      } else {
        System.err.println("Unknown option: " + args[2]);
        System.exit(1);
      }

      generator.setWriterVersion(WriterVersion.fromString(parquetVersion));
      generator.generateAll();
    } else if (command.equalsIgnoreCase("cleanup")) {
      generator.cleanup();
    } else {
      throw new IllegalArgumentException("invalid command " + command);
    }
  }
}
