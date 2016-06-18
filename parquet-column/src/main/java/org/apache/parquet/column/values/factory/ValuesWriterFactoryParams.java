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
package org.apache.parquet.column.values.factory;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import static org.apache.parquet.Preconditions.checkNotNull;

/**
 * Encapsulates parameters needed to create new ValuesWriter classes as part of
 * ValuesWriterFactory
 */
public class ValuesWriterFactoryParams {

  public ValuesWriterFactoryParams(
    WriterVersion writerVersion,
    int initialCapacity,
    int pageSize,
    ByteBufferAllocator allocator,
    boolean enableDictionary,
    int maxDictionaryByteSize) {
    this.writerVersion = checkNotNull(writerVersion, "writerVersion");
    this.initialCapacity = initialCapacity;
    this.pageSize = pageSize;
    this.allocator = checkNotNull(allocator, "allocator");
    this.enableDictionary = enableDictionary;
    this.maxDictionaryByteSize = maxDictionaryByteSize;
  }

  private final WriterVersion writerVersion;
  private final boolean enableDictionary;
  private final int initialCapacity;
  private final int pageSize;
  private final ByteBufferAllocator allocator;
  private final int maxDictionaryByteSize;

  public WriterVersion getWriterVersion() {
    return writerVersion;
  }

  public int getInitialCapacity() {
    return initialCapacity;
  }

  public int getPageSize() {
    return pageSize;
  }

  public ByteBufferAllocator getAllocator() {
    return allocator;
  }

  public int getMaxDictionaryByteSize() {
    return maxDictionaryByteSize;
  }

  public boolean getEnableDictionary() {
    return enableDictionary;
  }
}
