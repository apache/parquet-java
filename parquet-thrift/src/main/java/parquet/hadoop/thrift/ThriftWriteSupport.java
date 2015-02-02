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
package parquet.hadoop.thrift;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import com.twitter.elephantbird.pig.util.ThriftToPig;

import parquet.Log;
import parquet.hadoop.BadConfigurationException;
import parquet.hadoop.api.WriteSupport;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.ParquetEncodingException;
import parquet.io.api.RecordConsumer;
import parquet.pig.PigMetaData;
import parquet.schema.MessageType;
import parquet.thrift.ParquetWriteProtocol;
import parquet.thrift.ThriftMetaData;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftType.StructType;


public class ThriftWriteSupport<T extends TBase<?,?>> extends WriteSupport<T> {
  public static final String PARQUET_THRIFT_CLASS = "parquet.thrift.class";
  private static final Log LOG = Log.getLog(ThriftWriteSupport.class);

  public static <U extends TBase<?,?>> void setThriftClass(Configuration configuration, Class<U> thriftClass) {
    configuration.set(PARQUET_THRIFT_CLASS, thriftClass.getName());
  }

  public static Class<? extends TBase<?,?>> getThriftClass(Configuration configuration) {
    final String thriftClassName = configuration.get(PARQUET_THRIFT_CLASS);
    if (thriftClassName == null) {
      throw new BadConfigurationException("the thrift class conf is missing in job conf at " + PARQUET_THRIFT_CLASS);
    }
    try {
      @SuppressWarnings("unchecked")
      Class<? extends TBase<?,?>> thriftClass = (Class<? extends TBase<?,?>>)Class.forName(thriftClassName);
      return thriftClass;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("the class "+thriftClassName+" in job conf at " + PARQUET_THRIFT_CLASS + " could not be found", e);
    }
  }

  private MessageType schema;
  private StructType thriftStruct;
  private ParquetWriteProtocol parquetWriteProtocol;
  private WriteContext writeContext;

  /**
   * used from hadoop
   * the configuration must contain a thriftClass setting
   * @see ThriftWriteSupport#setThriftClass(Configuration, Class)
   */
  public ThriftWriteSupport() {
  }

  /**
   * @param thriftClass the thrift class used for writing values
   */
  public ThriftWriteSupport(Class<T> thriftClass) {
    init(thriftClass);
  }

  private <S extends TBase<?, ?>> void init(Class<S> thriftClass) {
    ThriftSchemaConverter thriftSchemaConverter = new ThriftSchemaConverter();
    this.thriftStruct = thriftSchemaConverter.toStructType(thriftClass);
    this.schema = thriftSchemaConverter.convert(thriftClass);
    final Map<String, String> extraMetaData = new ThriftMetaData(thriftClass.getName(), thriftStruct).toExtraMetaData();
    // adding the Pig schema as it would have been mapped from thrift
    if (isPigLoaded()) {
      new PigMetaData(new ThriftToPig<S>(thriftClass).toSchema()).addToMetaData(extraMetaData);
    }
    writeContext = new WriteContext(schema, extraMetaData);
  }

  private boolean isPigLoaded() {
    try {
      Class.forName("org.apache.pig.impl.logicalLayer.schema.Schema");
      return true;
    } catch (ClassNotFoundException e) {
      LOG.info("Pig is not loaded, pig metadata will not be written");
      return false;
    }
  }

  @Override
  public WriteContext init(Configuration configuration) {
    if (writeContext == null) {
      init(getThriftClass(configuration));
    }
    return writeContext;
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
