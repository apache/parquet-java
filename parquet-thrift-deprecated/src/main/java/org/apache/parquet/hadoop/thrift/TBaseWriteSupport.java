/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.thrift.ThriftSchemaConverter;
import org.apache.parquet.thrift.struct.ThriftType.StructType;

@Deprecated
public class TBaseWriteSupport<T extends TBase<?, ?>> extends AbstractThriftWriteSupport<T> {

  private static Configuration conf;

  public static <U extends TBase<?,?>> void setThriftClass(Configuration configuration, Class<U> thriftClass) {
    conf = configuration;
    AbstractThriftWriteSupport.setGenericThriftClass(configuration, thriftClass);
  }

  public static Class<? extends TBase<?,?>> getThriftClass(Configuration configuration) {
    return (Class<? extends TBase<?,?>>)AbstractThriftWriteSupport.getGenericThriftClass(configuration);
  }

  /**
   * used from hadoop
   * the configuration must contain a thriftClass setting
   * @see TBaseWriteSupport#setThriftClass(Configuration, Class)
   */
  public TBaseWriteSupport() {
  }

  public TBaseWriteSupport(Class<T> thriftClass) {
    super(thriftClass);
  }

  @Override
  public String getName() {
    return "thrift";
  }

  @Override
  protected StructType getThriftStruct() {
    ThriftSchemaConverter thriftSchemaConverter = new ThriftSchemaConverter(conf);
    return thriftSchemaConverter.toStructType((Class<TBase<?, ?>>)thriftClass);
  }

  @Override
  public void write(T record) {
    try {
      record.write(parquetWriteProtocol);
    } catch (TException e) {
      throw new ParquetEncodingException(e);
    }
  }
}
