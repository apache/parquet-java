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

package org.apache.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import org.apache.parquet.hadoop.util.HadoopCodecs;

import java.util.Map;

import static org.apache.parquet.hadoop.ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.DICTIONARY_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.BLOOM_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;
import static org.apache.parquet.hadoop.ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.RECORD_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.STATS_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.UnmaterializableRecordCounter.BAD_RECORD_THRESHOLD_CONF_KEY;

public class HadoopReadOptions extends ParquetReadOptions {
  private final Configuration conf;

  private static final String ALLOCATION_SIZE = "parquet.read.allocation.size";

  private HadoopReadOptions(boolean useSignedStringMinMax,
                            boolean useStatsFilter,
                            boolean useDictionaryFilter,
                            boolean useRecordFilter,
                            boolean useColumnIndexFilter,
                            boolean usePageChecksumVerification,
                            boolean useBloomFilter,
                            FilterCompat.Filter recordFilter,
                            MetadataFilter metadataFilter,
                            CompressionCodecFactory codecFactory,
                            ByteBufferAllocator allocator,
                            int maxAllocationSize,
                            Map<String, String> properties,
                            Configuration conf) {
    super(
        useSignedStringMinMax, useStatsFilter, useDictionaryFilter, useRecordFilter, useColumnIndexFilter,
        usePageChecksumVerification, useBloomFilter, recordFilter, metadataFilter, codecFactory, allocator,
        maxAllocationSize, properties
    );
    this.conf = conf;
  }

  @Override
  public String getProperty(String property) {
    String value = super.getProperty(property);
    if (value != null) {
      return value;
    }
    return conf.get(property);
  }

  public Configuration getConf() {
    return conf;
  }

  public static Builder builder(Configuration conf) {
    return new Builder(conf);
  }

  public static class Builder extends ParquetReadOptions.Builder {
    private final Configuration conf;

    public Builder(Configuration conf) {
      this.conf = conf;
      useSignedStringMinMax(conf.getBoolean("parquet.strings.signed-min-max.enabled", false));
      useDictionaryFilter(conf.getBoolean(DICTIONARY_FILTERING_ENABLED, true));
      useStatsFilter(conf.getBoolean(STATS_FILTERING_ENABLED, true));
      useRecordFilter(conf.getBoolean(RECORD_FILTERING_ENABLED, true));
      useColumnIndexFilter(conf.getBoolean(COLUMN_INDEX_FILTERING_ENABLED, true));
      usePageChecksumVerification(conf.getBoolean(PAGE_VERIFY_CHECKSUM_ENABLED,
        usePageChecksumVerification));
      useBloomFilter(conf.getBoolean(BLOOM_FILTERING_ENABLED, true));
      withCodecFactory(HadoopCodecs.newFactory(conf, 0));
      withRecordFilter(getFilter(conf));
      withMaxAllocationInBytes(conf.getInt(ALLOCATION_SIZE, 8388608));
      String badRecordThresh = conf.get(BAD_RECORD_THRESHOLD_CONF_KEY);
      if (badRecordThresh != null) {
        set(BAD_RECORD_THRESHOLD_CONF_KEY, badRecordThresh);
      }
    }

    @Override
    public ParquetReadOptions build() {
      return new HadoopReadOptions(
        useSignedStringMinMax, useStatsFilter, useDictionaryFilter, useRecordFilter,
        useColumnIndexFilter, usePageChecksumVerification, useBloomFilter, recordFilter, metadataFilter,
        codecFactory, allocator, maxAllocationSize, properties, conf);
    }
  }
}
