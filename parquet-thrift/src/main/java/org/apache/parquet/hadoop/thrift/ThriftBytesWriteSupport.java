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

import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.thrift.BufferedProtocolReadToWrite;
import org.apache.parquet.thrift.ParquetWriteProtocol;
import org.apache.parquet.thrift.ProtocolPipe;
import org.apache.parquet.thrift.ProtocolReadToWrite;
import org.apache.parquet.thrift.FieldIgnoredHandler;
import org.apache.parquet.thrift.ThriftSchemaConverter;
import org.apache.parquet.thrift.struct.ThriftType.StructType;

public class ThriftBytesWriteSupport extends WriteSupport<BytesWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftBytesWriteSupport.class);
  private static final String PARQUET_PROTOCOL_CLASS = "parquet.protocol.class";

  public static <U extends TProtocol> void setTProtocolClass(Configuration conf, Class<U> tProtocolClass) {
    conf.set(PARQUET_PROTOCOL_CLASS, tProtocolClass.getName());
  }

  public static Class<TProtocolFactory> getTProtocolFactoryClass(Configuration conf) {
    final String tProtocolClassName = conf.get(PARQUET_PROTOCOL_CLASS);
    if (tProtocolClassName == null) {
      throw new BadConfigurationException("the protocol class conf is missing in job conf at " + PARQUET_PROTOCOL_CLASS);
    }
    try {
      @SuppressWarnings("unchecked")
      Class<TProtocolFactory> tProtocolFactoryClass = (Class<TProtocolFactory>)Class.forName(tProtocolClassName + "$Factory");
      return tProtocolFactoryClass;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("the Factory for class " + tProtocolClassName + " in job conf at " + PARQUET_PROTOCOL_CLASS + " could not be found", e);
    }
  }

  private final boolean buffered;
  @SuppressWarnings("rawtypes") // TODO: fix type
  private final TBaseWriteSupport<?> thriftWriteSupport = new TBaseWriteSupport();
  private ProtocolPipe readToWrite;
  private TProtocolFactory protocolFactory;
  private Class<? extends TBase<?,?>> thriftClass;
  private MessageType schema;
  private StructType thriftStruct;
  private ParquetWriteProtocol parquetWriteProtocol;
  private final FieldIgnoredHandler errorHandler;
  private Configuration configuration;

  public ThriftBytesWriteSupport() {
    this.buffered = true;
    this.errorHandler = null;
  }

  /**
   * @deprecated Use @link{ThriftBytesWriteSupport(Configuration configuration,
   * TProtocolFactory protocolFactory, Class<\? extends TBase<\?, ?>> thriftClass,
   * boolean buffered, FieldIgnoredHandler errorHandler)} instead
   */
  @Deprecated
  public ThriftBytesWriteSupport(TProtocolFactory protocolFactory,
                                 Class<? extends TBase<?, ?>> thriftClass,
                                 boolean buffered,
                                 FieldIgnoredHandler errorHandler) {
    this(new Configuration(), protocolFactory, thriftClass, buffered, errorHandler);
  }

  public ThriftBytesWriteSupport(
      Configuration configuration,
      TProtocolFactory protocolFactory,
      Class<? extends TBase<?, ?>> thriftClass,
      boolean buffered,
      FieldIgnoredHandler errorHandler) {
    super();
    this.configuration = configuration;
    this.protocolFactory = protocolFactory;
    this.thriftClass = thriftClass;
    this.buffered = buffered;
    this.errorHandler = errorHandler;
    if (!buffered && errorHandler != null) {
      throw new IllegalArgumentException("Only buffered protocol can use error handler for now");
    }
  }

  @Override
  public String getName() {
    return "thrift";
  }

  @Override
  public WriteContext init(Configuration configuration) {
    this.configuration = configuration;
    if (this.protocolFactory == null) {
      try {
        this.protocolFactory = getTProtocolFactoryClass(configuration).newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    if (thriftClass != null) {
      TBaseWriteSupport.setThriftClass(configuration, thriftClass);
    } else {
      thriftClass = TBaseWriteSupport.getThriftClass(configuration);
    }
    ThriftSchemaConverter thriftSchemaConverter = new ThriftSchemaConverter(this.configuration);
    thriftStruct = thriftSchemaConverter.toStructType(thriftClass);
    schema = thriftSchemaConverter.convert(thriftStruct);
    if (buffered) {
      readToWrite = new BufferedProtocolReadToWrite(thriftStruct, errorHandler);
    } else {
      readToWrite = new ProtocolReadToWrite();
    }
    return thriftWriteSupport.init(configuration);
  }

  private static Method SET_READ_LENGTH;
  static {
    try {
      SET_READ_LENGTH = TBinaryProtocol.class.getMethod("setReadLength", int.class);
    } catch (NoSuchMethodException e) {
      SET_READ_LENGTH = null;
    }
  }

  private TProtocol protocol(BytesWritable record) {
    TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(new ByteArrayInputStream(record.getBytes())));

    /* Reduce the chance of OOM when data is corrupted. When readBinary is called on TBinaryProtocol, it reads the length of the binary first,
     so if the data is corrupted, it could read a big integer as the length of the binary and therefore causes OOM to happen.
     Currently this fix only applies to TBinaryProtocol which has the setReadLength defined (thrift 0.7).
      */
    if (SET_READ_LENGTH != null && protocol instanceof TBinaryProtocol) {
      try {
        SET_READ_LENGTH.invoke(protocol, new Object[]{record.getLength()});
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        LOG.warn("setReadLength should not throw an exception", e);
        SET_READ_LENGTH = null;
      }
    }

    return protocol;
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    parquetWriteProtocol = new ParquetWriteProtocol(
        configuration, recordConsumer, columnIO, thriftStruct);
    thriftWriteSupport.prepareForWrite(recordConsumer);
  }

  @Override
  public void write(BytesWritable record) {
    try {
      readToWrite.readOne(protocol(record), parquetWriteProtocol);
    } catch (TException e) {
      throw new ParquetEncodingException(e);
    }
  }

}
