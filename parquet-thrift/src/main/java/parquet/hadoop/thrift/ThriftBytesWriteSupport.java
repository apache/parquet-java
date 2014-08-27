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

import java.io.ByteArrayInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;

import parquet.hadoop.BadConfigurationException;
import parquet.hadoop.api.WriteSupport;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.ParquetEncodingException;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.thrift.BufferedProtocolReadToWrite;
import parquet.thrift.ParquetWriteProtocol;
import parquet.thrift.ProtocolPipe;
import parquet.thrift.ProtocolReadToWrite;
import parquet.thrift.FieldIgnoredHandler;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftType.StructType;

public class ThriftBytesWriteSupport extends WriteSupport<BytesWritable> {
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

  public ThriftBytesWriteSupport() {
    this.buffered = true;
    this.errorHandler = null;
  }

  public ThriftBytesWriteSupport(TProtocolFactory protocolFactory, Class<? extends TBase<?, ?>> thriftClass, boolean buffered, FieldIgnoredHandler errorHandler) {
    super();
    this.protocolFactory = protocolFactory;
    this.thriftClass = thriftClass;
    this.buffered = buffered;
    this.errorHandler = errorHandler;
    if (!buffered && errorHandler != null) {
      throw new IllegalArgumentException("Only buffered protocol can use error handler for now");
    }
  }

  @Override
  public WriteContext init(Configuration configuration) {
    if (this.protocolFactory == null) {
      try {
        this.protocolFactory = getTProtocolFactoryClass(configuration).newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    if (thriftClass != null) {
      TBaseWriteSupport.setTBaseClass(configuration, thriftClass);
    } else {
      thriftClass = (Class<? extends TBase<?,?>>)ThriftWriteSupport.getThriftClass(configuration);
    }
    ThriftSchemaConverter thriftSchemaConverter = new ThriftSchemaConverter();
    this.thriftStruct = thriftSchemaConverter.toStructType(thriftClass);
    this.schema = thriftSchemaConverter.convert(thriftStruct);
    if (buffered) {
      readToWrite = new BufferedProtocolReadToWrite(thriftStruct, errorHandler);
    } else {
      readToWrite = new ProtocolReadToWrite();
    }
    return thriftWriteSupport.init(configuration);
  }

  private TProtocol protocol(BytesWritable record) {
    TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(new ByteArrayInputStream(record.getBytes())));

    /* Reduce the chance of OOM when data is corrupted. When readBinary is called on TBinaryProtocol, it reads the length of the binary first,
     so if the data is corrupted, it could read a big integer as the length of the binary and therefore causes OOM to happen.
     Currently this fix only applies to TBinaryProtocol which has the setReadLength defined.
      */
    if (protocol instanceof TBinaryProtocol) {
      ((TBinaryProtocol)protocol).setReadLength(record.getLength());
    }
    return protocol;
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    this.parquetWriteProtocol = new ParquetWriteProtocol(recordConsumer, columnIO, thriftStruct);
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
