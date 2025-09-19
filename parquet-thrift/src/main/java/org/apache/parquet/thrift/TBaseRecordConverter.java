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
package org.apache.parquet.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

public class TBaseRecordConverter<T extends TBase<?, ?>> extends ThriftRecordConverter<T> {

  @SuppressWarnings("unused")
  public TBaseRecordConverter(
      final Class<T> thriftClass, MessageType requestedParquetSchema, StructType thriftType, Configuration conf) {
    this(thriftClass, requestedParquetSchema, thriftType, new HadoopParquetConfiguration(conf));
  }

  public TBaseRecordConverter(
      final Class<T> thriftClass,
      MessageType requestedParquetSchema,
      StructType thriftType,
      ParquetConfiguration conf) {
    super(
        new ThriftReader<T>() {
          @Override
          public T readOneRecord(TProtocol protocol) throws TException {
            try {
              T thriftObject = thriftClass.newInstance();
              thriftObject.read(protocol);
              return thriftObject;
            } catch (InstantiationException e) {
              throw new ParquetDecodingException("Could not instantiate Thrift " + thriftClass, e);
            } catch (IllegalAccessException e) {
              throw new ParquetDecodingException(
                  "Thrift class or constructor not public " + thriftClass, e);
            }
          }
        },
        thriftClass.getSimpleName(),
        requestedParquetSchema,
        thriftType,
        conf);
  }
}
