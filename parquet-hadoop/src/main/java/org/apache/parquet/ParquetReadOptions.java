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

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.util.HadoopCodecs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

// Internal use only
public class ParquetReadOptions {
  private static final boolean RECORD_FILTERING_ENABLED_DEFAULT = true;
  private static final boolean STATS_FILTERING_ENABLED_DEFAULT = true;
  private static final boolean DICTIONARY_FILTERING_ENABLED_DEFAULT = true;
  private static final boolean COLUMN_INDEX_FILTERING_ENABLED_DEFAULT = true;
  private static final int ALLOCATION_SIZE_DEFAULT = 8388608; // 8MB
  private static final boolean PAGE_VERIFY_CHECKSUM_ENABLED_DEFAULT = false;

  private final boolean useSignedStringMinMax;
  private final boolean useStatsFilter;
  private final boolean useDictionaryFilter;
  private final boolean useRecordFilter;
  private final boolean useColumnIndexFilter;
  private final boolean usePageChecksumVerification;
  private final FilterCompat.Filter recordFilter;
  private final ParquetMetadataConverter.MetadataFilter metadataFilter;
  private final CompressionCodecFactory codecFactory;
  private final ByteBufferAllocator allocator;
  private final int maxAllocationSize;
  private final Map<String, String> properties;

  ParquetReadOptions(boolean useSignedStringMinMax,
                     boolean useStatsFilter,
                     boolean useDictionaryFilter,
                     boolean useRecordFilter,
                     boolean useColumnIndexFilter,
                     boolean usePageChecksumVerification,
                     FilterCompat.Filter recordFilter,
                     ParquetMetadataConverter.MetadataFilter metadataFilter,
                     CompressionCodecFactory codecFactory,
                     ByteBufferAllocator allocator,
                     int maxAllocationSize,
                     Map<String, String> properties) {
    this.useSignedStringMinMax = useSignedStringMinMax;
    this.useStatsFilter = useStatsFilter;
    this.useDictionaryFilter = useDictionaryFilter;
    this.useRecordFilter = useRecordFilter;
    this.useColumnIndexFilter = useColumnIndexFilter;
    this.usePageChecksumVerification = usePageChecksumVerification;
    this.recordFilter = recordFilter;
    this.metadataFilter = metadataFilter;
    this.codecFactory = codecFactory;
    this.allocator = allocator;
    this.maxAllocationSize = maxAllocationSize;
    this.properties = Collections.unmodifiableMap(properties);
  }

  public boolean useSignedStringMinMax() {
    return useSignedStringMinMax;
  }

  public boolean useStatsFilter() {
    return useStatsFilter;
  }

  public boolean useDictionaryFilter() {
    return useDictionaryFilter;
  }

  public boolean useRecordFilter() {
    return useRecordFilter;
  }

  public boolean useColumnIndexFilter() {
    return useColumnIndexFilter;
  }

  public boolean usePageChecksumVerification() {
    return usePageChecksumVerification;
  }

  public FilterCompat.Filter getRecordFilter() {
    return recordFilter;
  }

  public ParquetMetadataConverter.MetadataFilter getMetadataFilter() {
    return metadataFilter;
  }

  public CompressionCodecFactory getCodecFactory() {
    return codecFactory;
  }

  public ByteBufferAllocator getAllocator() {
    return allocator;
  }

  public int getMaxAllocationSize() {
    return maxAllocationSize;
  }

  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  public String getProperty(String property) {
    return properties.get(property);
  }

  public boolean isEnabled(String property, boolean defaultValue) {
    Optional<String> propValue = Optional.ofNullable(properties.get(property));
    return propValue.isPresent() ? Boolean.valueOf(propValue.get())
        : defaultValue;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    protected boolean useSignedStringMinMax = false;
    protected boolean useStatsFilter = STATS_FILTERING_ENABLED_DEFAULT;
    protected boolean useDictionaryFilter = DICTIONARY_FILTERING_ENABLED_DEFAULT;
    protected boolean useRecordFilter = RECORD_FILTERING_ENABLED_DEFAULT;
    protected boolean useColumnIndexFilter = COLUMN_INDEX_FILTERING_ENABLED_DEFAULT;
    protected boolean usePageChecksumVerification = PAGE_VERIFY_CHECKSUM_ENABLED_DEFAULT;
    protected FilterCompat.Filter recordFilter = null;
    protected ParquetMetadataConverter.MetadataFilter metadataFilter = NO_FILTER;
    // the page size parameter isn't used when only using the codec factory to get decompressors
    protected CompressionCodecFactory codecFactory = HadoopCodecs.newFactory(0);
    protected ByteBufferAllocator allocator = new HeapByteBufferAllocator();
    protected int maxAllocationSize = ALLOCATION_SIZE_DEFAULT;
    protected Map<String, String> properties = new HashMap<>();

    public Builder useSignedStringMinMax(boolean useSignedStringMinMax) {
      this.useSignedStringMinMax = useSignedStringMinMax;
      return this;
    }

    public Builder useSignedStringMinMax() {
      this.useSignedStringMinMax = true;
      return this;
    }

    public Builder useStatsFilter(boolean useStatsFilter) {
      this.useStatsFilter = useStatsFilter;
      return this;
    }

    public Builder useStatsFilter() {
      this.useStatsFilter = true;
      return this;
    }

    public Builder useDictionaryFilter(boolean useDictionaryFilter) {
      this.useDictionaryFilter = useDictionaryFilter;
      return this;
    }

    public Builder useDictionaryFilter() {
      this.useDictionaryFilter = true;
      return this;
    }

    public Builder useRecordFilter(boolean useRecordFilter) {
      this.useRecordFilter = useRecordFilter;
      return this;
    }

    public Builder useRecordFilter() {
      this.useRecordFilter = true;
      return this;
    }

    public Builder useColumnIndexFilter(boolean useColumnIndexFilter) {
      this.useColumnIndexFilter = useColumnIndexFilter;
      return this;
    }

    public Builder useColumnIndexFilter() {
      return useColumnIndexFilter(true);
    }


    public Builder usePageChecksumVerification(boolean usePageChecksumVerification) {
      this.usePageChecksumVerification = usePageChecksumVerification;
      return this;
    }

    public Builder usePageChecksumVerification() {
      return usePageChecksumVerification(true);
    }

    public Builder withRecordFilter(FilterCompat.Filter rowGroupFilter) {
      this.recordFilter = rowGroupFilter;
      return this;
    }

    public Builder withRange(long start, long end) {
      this.metadataFilter = ParquetMetadataConverter.range(start, end);
      return this;
    }

    public Builder withOffsets(long... rowGroupOffsets) {
      this.metadataFilter = ParquetMetadataConverter.offsets(rowGroupOffsets);
      return this;
    }

    public Builder withMetadataFilter(ParquetMetadataConverter.MetadataFilter metadataFilter) {
      this.metadataFilter = metadataFilter;
      return this;
    }

    public Builder withCodecFactory(CompressionCodecFactory codecFactory) {
      this.codecFactory = codecFactory;
      return this;
    }

    public Builder withAllocator(ByteBufferAllocator allocator) {
      this.allocator = allocator;
      return this;
    }

    public Builder withMaxAllocationInBytes(int allocationSizeInBytes) {
      this.maxAllocationSize = allocationSizeInBytes;
      return this;
    }

    public Builder withPageChecksumVerification(boolean val) {
      this.usePageChecksumVerification = val;
      return this;
    }

    public Builder set(String key, String value) {
      properties.put(key, value);
      return this;
    }

    public Builder copy(ParquetReadOptions options) {
      useSignedStringMinMax(options.useSignedStringMinMax);
      useStatsFilter(options.useStatsFilter);
      useDictionaryFilter(options.useDictionaryFilter);
      useRecordFilter(options.useRecordFilter);
      withRecordFilter(options.recordFilter);
      withMetadataFilter(options.metadataFilter);
      withCodecFactory(options.codecFactory);
      withAllocator(options.allocator);
      withPageChecksumVerification(options.usePageChecksumVerification);
      for (Map.Entry<String, String> keyValue : options.properties.entrySet()) {
        set(keyValue.getKey(), keyValue.getValue());
      }
      return this;
    }

    public ParquetReadOptions build() {
      return new ParquetReadOptions(
        useSignedStringMinMax, useStatsFilter, useDictionaryFilter, useRecordFilter,
        useColumnIndexFilter, usePageChecksumVerification, recordFilter, metadataFilter,
        codecFactory, allocator, maxAllocationSize, properties);
    }
  }
}
