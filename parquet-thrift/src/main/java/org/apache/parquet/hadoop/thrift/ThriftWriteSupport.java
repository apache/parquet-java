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
package org.apache.parquet.hadoop.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;

import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;

/**
 * @deprecated This class is replaced by TBaseWriteSupport.
 */
@Deprecated
public class ThriftWriteSupport<T extends TBase<?, ?>> extends WriteSupport<T> {
  public static final String PARQUET_THRIFT_CLASS = AbstractThriftWriteSupport.PARQUET_THRIFT_CLASS;

  public static <U extends TBase<?, ?>> void setThriftClass(Configuration configuration, Class<U> thriftClass) {
    TBaseWriteSupport.setThriftClass(configuration, thriftClass);
  }

  public static Class<? extends TBase<?, ?>> getThriftClass(Configuration configuration) {
    return TBaseWriteSupport.getThriftClass(configuration);
  }

  private TBaseWriteSupport writeSupport;

  /**
   * used from hadoop the configuration must contain a thriftClass setting
   * 
   * @see ThriftWriteSupport#setThriftClass(Configuration, Class)
   */
  public ThriftWriteSupport() {
    this.writeSupport = new TBaseWriteSupport();
  }

  /**
   * @param thriftClass the thrift class used for writing values
   */
  public ThriftWriteSupport(Class<T> thriftClass) {
    this.writeSupport = new TBaseWriteSupport(thriftClass);
  }

  @Override
  public String getName() {
    return writeSupport.getName();
  }

  @Override
  public WriteContext init(Configuration configuration) {
    return this.writeSupport.init(configuration);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.writeSupport.prepareForWrite(recordConsumer);
  }

  @Override
  public void write(T record) {
    this.writeSupport.write(record);
  }
}
