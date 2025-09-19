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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.thrift.FieldIgnoredHandler;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

/**
 * Output format that turns Thrift bytes into Parquet format using the thrift TProtocol layer
 */
public class ParquetThriftBytesOutputFormat extends ParquetOutputFormat<BytesWritable> {

  public static void setThriftClass(Job job, Class<? extends TBase<?, ?>> thriftClass) {
    TBaseWriteSupport.setThriftClass(ContextUtil.getConfiguration(job), thriftClass);
  }

  public static Class<? extends TBase<?, ?>> getThriftClass(Job job) {
    return TBaseWriteSupport.getThriftClass(ContextUtil.getConfiguration(job));
  }

  public static <U extends TProtocol> void setTProtocolClass(Job job, Class<U> tProtocolClass) {
    ThriftBytesWriteSupport.setTProtocolClass(ContextUtil.getConfiguration(job), tProtocolClass);
  }

  /**
   * Used when settings are set in the configuration
   */
  public ParquetThriftBytesOutputFormat() {
    super(new ThriftBytesWriteSupport());
  }

  /**
   * The buffered implementation will buffer each record and deal with invalid records (more expansive).
   * when catching an exception the record can be discarded.
   * The non-buffered implementation will stream field by field. Exceptions are unrecoverable and the file
   * must be closed when an invalid record is written.
   *
   * @param configuration   configuration
   * @param protocolFactory the protocol factory to use to read the bytes
   * @param thriftClass     thriftClass the class to extract the schema from
   * @param buffered        whether we should buffer each record
   * @param errorHandler    handle record corruption and schema incompatible exception
   */
  public ParquetThriftBytesOutputFormat(
      Configuration configuration,
      TProtocolFactory protocolFactory,
      Class<? extends TBase<?, ?>> thriftClass,
      boolean buffered,
      FieldIgnoredHandler errorHandler) {
    super(new ThriftBytesWriteSupport(configuration, protocolFactory, thriftClass, buffered, errorHandler));
  }
}
