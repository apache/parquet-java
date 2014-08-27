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
package parquet.hadoop.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import parquet.hadoop.BadConfigurationException;
import parquet.io.ParquetEncodingException;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftType.StructType;

public class TBaseWriteSupport<T extends TBase<?, ?>> extends ThriftWriteSupport<T> {

  public static void setTBaseClass(Configuration configuration, Class<? extends TBase<?,?>> thriftClass) {
    ThriftWriteSupport.setThriftClass(configuration, thriftClass);
  }

  public static Class<? extends TBase<?,?>> getTBaseClass(Configuration configuration) {
    return (Class<? extends TBase<?,?>>)ThriftWriteSupport.getThriftClass(configuration);
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
  protected StructType getThriftStruct() {
    ThriftSchemaConverter thriftSchemaConverter = new ThriftSchemaConverter();
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
