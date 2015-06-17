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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocolFactory;

import org.apache.parquet.thrift.FieldIgnoredHandler;

/**
 * To create a Parquet file from the Thrift binary of records
 */
public class ThriftToParquetFileWriter implements Closeable {

  private final RecordWriter<Void, BytesWritable> recordWriter;
  private final TaskAttemptContext taskAttemptContext;

  /**
   * defaults to buffered = true
   * @param fileToCreate the file to create. If null will create the default file name from the taskAttemptContext
   * @param taskAttemptContext The current taskAttemptContext
   * @param protocolFactory to create protocols to read the incoming bytes
   * @param thriftClass to produce the schema
   * @throws IOException if there was a problem writing
   * @throws InterruptedException from the underlying Hadoop API
   */
  public ThriftToParquetFileWriter(
      Path fileToCreate,
      TaskAttemptContext taskAttemptContext,
      TProtocolFactory protocolFactory,
      Class<? extends TBase<?,?>> thriftClass)
          throws IOException, InterruptedException {
    this(fileToCreate, taskAttemptContext, protocolFactory, thriftClass, true, null);
  }

  /**
   * defaults to buffered = true
   * @param fileToCreate the file to create. If null will create the default file name from the taskAttemptContext
   * @param taskAttemptContext The current taskAttemptContext
   * @param protocolFactory to create protocols to read the incoming bytes
   * @param thriftClass to produce the schema
   * @param errorHandler to define what to do when failing to read a record
   * @throws IOException if there was a problem writing
   * @throws InterruptedException from the underlying Hadoop API
   */
  public ThriftToParquetFileWriter(
      Path fileToCreate,
      TaskAttemptContext taskAttemptContext,
      TProtocolFactory protocolFactory,
      Class<? extends TBase<?,?>> thriftClass,
      FieldIgnoredHandler errorHandler) throws IOException, InterruptedException {
    this(fileToCreate, taskAttemptContext, protocolFactory, thriftClass, true, errorHandler);
  }

  /**
   * @param fileToCreate the file to create. If null will create the default file name from the taskAttemptContext
   * @param taskAttemptContext The current taskAttemptContext
   * @param protocolFactory to create protocols to read the incoming bytes
   * @param thriftClass to produce the schema
   * @param buffered buffer each record individually
   * @param errorHandler an error handler
   * @throws IOException if there was a problem writing
   * @throws InterruptedException from the underlying Hadoop API
   */
  public ThriftToParquetFileWriter(
      Path fileToCreate,
      TaskAttemptContext taskAttemptContext,
      TProtocolFactory protocolFactory,
      Class<? extends TBase<?,?>> thriftClass,
      boolean buffered,
      FieldIgnoredHandler errorHandler) throws IOException, InterruptedException {
    this.taskAttemptContext = taskAttemptContext;
    this.recordWriter = new ParquetThriftBytesOutputFormat(
        taskAttemptContext.getConfiguration(), protocolFactory, thriftClass, buffered, errorHandler)
        .getRecordWriter(taskAttemptContext, fileToCreate);
  }

  /**
   * write one record to the columnar store
   * @param bytes a bytes writable
   * @throws IOException if there is an error while writing
   * @throws InterruptedException if writing is interrupted
   */
  public void write(BytesWritable bytes) throws IOException, InterruptedException {
    recordWriter.write(null, bytes);
  }

  /**
   * close the file
   *
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    try {
      recordWriter.close(taskAttemptContext);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new IOException("The thread was interrupted", e);
    }
  }
}
