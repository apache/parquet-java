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

import static org.apache.parquet.benchmarks.BenchmarkConstants.FIXED_LEN_BYTEARRAY_SIZE;
import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;

public class BenchmarkFiles {
  public static final Configuration defaultConfiguration = new Configuration();
  public static final Configuration readAllPrimitivesConfiguration = new Configuration();
  public static final Configuration readFourPrimitivesConfiguration = new Configuration();
  public static final Configuration readOnePrimitiveConfiguration = new Configuration();
  public static final Configuration flbaReadConfiguration = new Configuration();

  static {
    readOnePrimitiveConfiguration.set(PARQUET_READ_SCHEMA, "message test { required int32 int32_field;}");
    flbaReadConfiguration.set(PARQUET_READ_SCHEMA, "message test { required fixed_len_byte_array(" + FIXED_LEN_BYTEARRAY_SIZE + ") flba_field;}");

    //the complete schema minus the binary field
    readAllPrimitivesConfiguration.set(PARQUET_READ_SCHEMA, "message test { "
            + "required int32 int32_field; "
            + "required int64 int64_field; "
            + "required boolean boolean_field; "
            + "required float float_field; "
            + "required double double_field; "
            + "required fixed_len_byte_array(" + FIXED_LEN_BYTEARRAY_SIZE +") flba_field; "
            + "required int96 int96_field; "
            + "} ");

    readFourPrimitivesConfiguration.set(PARQUET_READ_SCHEMA, "message test { "
            + "required int32 int32_field; "
            + "required int64 int64_field; "
            + "required boolean boolean_field; "
            + "required float float_field; "
            + "} ");
  }

  public static final String TARGET_DIR = "target/tests/ParquetBenchmarks";
  public static final Path file_1M = new Path(TARGET_DIR + "/PARQUET-1M");

  //different block and page sizes
  public static final Path file_1M_BS256M_PS4M = new Path(TARGET_DIR + "/PARQUET-1M-BS256M_PS4M");
  public static final Path file_1M_BS256M_PS8M = new Path(TARGET_DIR + "/PARQUET-1M-BS256M_PS8M");
  public static final Path file_1M_BS512M_PS4M = new Path(TARGET_DIR + "/PARQUET-1M-BS512M_PS4M");
  public static final Path file_1M_BS512M_PS8M = new Path(TARGET_DIR + "/PARQUET-1M-BS512M_PS8M");

  //different compression codecs
//  public final Path parquetFile_1M_LZO = new Path("target/tests/ParquetBenchmarks/PARQUET-1M-LZO");
  public static final Path file_1M_SNAPPY = new Path(TARGET_DIR + "/PARQUET-1M-SNAPPY");
  public static final Path file_1M_GZIP = new Path(TARGET_DIR + "/PARQUET-1M-GZIP");
  public static final Path file_10M_GZIP = new Path(TARGET_DIR + "/PARQUET-10M-GZIP");
}
