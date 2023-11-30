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
package org.apache.parquet.hadoop.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.io.api.RecordConsumer;

/**
 * Helps composing write supports
 *
 * @param <T> the Java class of objects written with this WriteSupport
 */
public class DelegatingWriteSupport<T> extends WriteSupport<T> {

  private final WriteSupport<T> delegate;

  public DelegatingWriteSupport(WriteSupport<T> delegate) {
    super();
    this.delegate = delegate;
  }

  @Override
  public WriteSupport.WriteContext init(Configuration configuration) {
    return delegate.init(configuration);
  }

  @Override
  public WriteSupport.WriteContext init(ParquetConfiguration configuration) {
    return delegate.init(configuration);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    delegate.prepareForWrite(recordConsumer);
  }

  @Override
  public void write(T record) {
    delegate.write(record);
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    return delegate.finalizeWrite();
  }

  @Override
  public String toString() {
    return getClass().getName() + "(" + delegate.toString() + ")";
  }
}
