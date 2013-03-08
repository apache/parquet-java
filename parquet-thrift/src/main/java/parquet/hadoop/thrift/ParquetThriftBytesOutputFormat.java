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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import parquet.hadoop.ParquetOutputFormat;

public class ParquetThriftBytesOutputFormat extends ParquetOutputFormat<BytesWritable> {

  public static void setThriftClass(Job job, Class<? extends TBase<?, ?>> thriftClass) {
    ThriftWriteSupport.setThriftClass(job.getConfiguration(), thriftClass);
  }

  public static Class<? extends TBase<?,?>> getThriftClass(Job job) {
    return ThriftWriteSupport.getThriftClass(job.getConfiguration());
  }

  public static <U extends TProtocol> void setTProtocolClass(Job job, Class<U> tProtocolClass) {
    ThriftBytesWriteSupport.setTProtocolClass(job.getConfiguration(), tProtocolClass);
  }

  public ParquetThriftBytesOutputFormat() {
    super(new ThriftBytesWriteSupport());
  }

  public ParquetThriftBytesOutputFormat(TProtocolFactory protocolFactory, Class<? extends TBase<?, ?>> thriftClass) {
    super(new ThriftBytesWriteSupport(protocolFactory, thriftClass));
  }

}
