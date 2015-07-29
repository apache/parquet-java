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

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

public class BenchmarkConstants {
  public static final int ONE_K = 1000;
  public static final int FIVE_K = 5 * ONE_K;
  public static final int TEN_K = 2 * FIVE_K;
  public static final int HUNDRED_K = 10 * TEN_K;
  public static final int ONE_MILLION = 10 * HUNDRED_K;
  public static final int TEN_MILLION = 10 * ONE_MILLION;

  public static final int FIXED_LEN_BYTEARRAY_SIZE = 1024;

  public static final int BLOCK_SIZE_DEFAULT = DEFAULT_BLOCK_SIZE;
  public static final int BLOCK_SIZE_256M = 256 * 1024 * 1024;
  public static final int BLOCK_SIZE_512M = 512 * 1024 * 1024;

  public static final int PAGE_SIZE_DEFAULT = DEFAULT_PAGE_SIZE;
  public static final int PAGE_SIZE_4M = 4 * 1024 * 1024;
  public static final int PAGE_SIZE_8M = 8 * 1024 * 1024;

  public static final int DICT_PAGE_SIZE = 512;
}
