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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocolFactory;

/**
 * To create a Parquet file from the Thrift binary of records
 *
 * @author Julien Le Dem
 *
 */
public class ThriftToParquetFileWriter {

  private final RecordWriter<Void, BytesWritable> recordWriter;
  private final TaskAttemptContext taskAttemptContext;

  /**
   * @param fileToCreate the file to create. If null will create the default file name from the taskAttemptContext
   * @param taskAttemptContext The current taskAttemptContext
   * @param protocolFactory to create protocols to read the incoming bytes
   * @param thriftClass to produce the schema
   * @throws IOException if there was a problem writing
   * @throws InterruptedException from the underlying Hadoop API
   */
  public ThriftToParquetFileWriter(Path fileToCreate, TaskAttemptContext taskAttemptContext, TProtocolFactory protocolFactory, Class<? extends TBase<?,?>> thriftClass) throws IOException, InterruptedException {
    this.taskAttemptContext = taskAttemptContext;
    this.recordWriter = new ParquetThriftBytesOutputFormat(protocolFactory, thriftClass).getRecordWriter(taskAttemptContext, fileToCreate);
  }

  public void write(BytesWritable bytes) throws IOException, InterruptedException {
    recordWriter.write(null, bytes);
  }

  public void close() throws IOException, InterruptedException {
    recordWriter.close(taskAttemptContext);
  }
}
