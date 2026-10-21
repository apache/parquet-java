/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.conf;

/**
 * Stores the read configuration property keys used to configure how Parquet
 * files are read. This class contains only constants and is decoupled from the
 * legacy Hadoop {@code Configuration} and {@code InputFormat} machinery, so that
 * the configuration properties can be used without loading {@code FileInputFormat}
 * and its transitive dependencies.
 */
public final class ParquetInputProperties {

  /** key to configure the ReadSupport implementation */
  public static final String READ_SUPPORT_CLASS = "parquet.read.support.class";

  /** key to configure the filter */
  public static final String UNBOUND_RECORD_FILTER = "parquet.read.filter";

  /** key to configure type checking for conflicting schemas (default: true) */
  public static final String STRICT_TYPE_CHECKING = "parquet.strict.typing";

  /** key to configure the filter predicate */
  public static final String FILTER_PREDICATE = "parquet.private.read.filter.predicate";

  /** key to configure whether record-level filtering is enabled */
  public static final String RECORD_FILTERING_ENABLED = "parquet.filter.record-level.enabled";

  /** key to configure whether row group stats filtering is enabled */
  public static final String STATS_FILTERING_ENABLED = "parquet.filter.stats.enabled";

  /** key to configure whether row group dictionary filtering is enabled */
  public static final String DICTIONARY_FILTERING_ENABLED = "parquet.filter.dictionary.enabled";

  /** key to configure whether column index filtering of pages is enabled */
  public static final String COLUMN_INDEX_FILTERING_ENABLED = "parquet.filter.columnindex.enabled";

  /** key to configure whether page level checksum verification is enabled */
  public static final String PAGE_VERIFY_CHECKSUM_ENABLED = "parquet.page.verify-checksum.enabled";

  /** key to configure whether row group bloom filtering is enabled */
  public static final String BLOOM_FILTERING_ENABLED = "parquet.filter.bloom.enabled";

  /** Key to configure if off-heap buffer should be used for decryption */
  public static final String OFF_HEAP_DECRYPT_BUFFER_ENABLED = "parquet.decrypt.off-heap.buffer.enabled";

  /** Key to enable/disable vectored io while reading parquet files: {@value}. */
  public static final String HADOOP_VECTORED_IO_ENABLED = "parquet.hadoop.vectored.io.enabled";

  /** Default value of parquet.hadoop.vectored.io.enabled is {@value}. */
  public static final boolean HADOOP_VECTORED_IO_DEFAULT = true;

  private ParquetInputProperties() {}
}
