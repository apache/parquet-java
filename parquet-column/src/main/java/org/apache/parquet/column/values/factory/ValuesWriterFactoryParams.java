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

    this.writerVersion = writerVersion;
    this.initialCapacity = initialCapacity;
    this.pageSize = pageSize;
    this.allocator = allocator;
    this.enableDictionary = enableDictionary;
    this.maxDictionaryByteSize = maxDictionaryByteSize;
  }

  private WriterVersion writerVersion;
  public WriterVersion getWriterVersion() {
    return writerVersion;
  }

  private int initialCapacity;
  public int getInitialCapacity() {
    return initialCapacity;
  }

  private int pageSize;
  public int getPageSize() {
    return pageSize;
  }

  private ByteBufferAllocator allocator;
  public ByteBufferAllocator getAllocator() {
    return allocator;
  }

  private int maxDictionaryByteSize;
  public int getMaxDictionaryByteSize() {
    return maxDictionaryByteSize;
  }

  private boolean enableDictionary;
  public boolean getEnableDictionary() {
    return enableDictionary;
  }
}
