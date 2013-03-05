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
import parquet.hadoop.WriteSupport;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.ParquetEncodingException;
import parquet.io.RecordConsumer;
import parquet.schema.MessageType;
import parquet.thrift.ParquetWriteProtocol;
import parquet.thrift.ThriftMetaData;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftType.StructType;


public class ThriftWriteSupport<T extends TBase> extends WriteSupport<T> {
  public static final String PARQUET_THRIFT_CLASS = "parquet.thrift.class";

  public static <U extends TBase<?,?>> void setThriftClass(Configuration configuration, Class<U> thriftClass) {
    configuration.set(PARQUET_THRIFT_CLASS, thriftClass.getName());
  }

  public static Class<TBase<?,?>> getThriftClass(Configuration configuration) {
    final String thriftClassName = configuration.get(PARQUET_THRIFT_CLASS);
    if (thriftClassName == null) {
      throw new BadConfigurationException("the thrift class conf is missing in job conf at " + PARQUET_THRIFT_CLASS);
    }
    try {
      @SuppressWarnings("unchecked")
      Class<TBase<?,?>> thriftClass = (Class<TBase<?,?>>)Class.forName(thriftClassName);
      return thriftClass;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("the class "+thriftClassName+" in job conf at " + PARQUET_THRIFT_CLASS + " could not be found", e);
    }
  }

  private MessageType schema;
  private StructType thriftStruct;
  private ParquetWriteProtocol parquetWriteProtocol;

  @Override
  public WriteContext init(Configuration configuration) {
    Class<TBase<?,?>> thriftClass = getThriftClass(configuration);
    ThriftSchemaConverter thriftSchemaConverter = new ThriftSchemaConverter();
    this.thriftStruct = thriftSchemaConverter.toStructType(thriftClass);
    this.schema = thriftSchemaConverter.convert(thriftClass);
    return new WriteContext(schema, new ThriftMetaData(thriftClass, thriftStruct).toExtraMetaData());
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    this.parquetWriteProtocol = new ParquetWriteProtocol(recordConsumer, columnIO, thriftStruct);
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
