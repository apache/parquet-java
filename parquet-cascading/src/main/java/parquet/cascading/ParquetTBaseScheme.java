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
package parquet.cascading;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.thrift.TBase;

import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import parquet.hadoop.thrift.ParquetThriftInputFormat;
import parquet.hadoop.thrift.ThriftReadSupport;
import parquet.hadoop.thrift.ThriftWriteSupport;
import parquet.thrift.TBaseRecordConverter;
import cascading.flow.FlowProcess;
import cascading.tap.Tap;

public class ParquetTBaseScheme<T extends TBase<?,?>> extends ParquetValueScheme<T> {

  private Class<T> thriftClass;

  // In the case of reads, we can read the thrift class from the file metadata
  public ParquetTBaseScheme() {
  }

  public ParquetTBaseScheme(Class<T> thriftClass) {
    this.thriftClass = thriftClass;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sourceConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
    ParquetInputFormat.setReadSupportClass(jobConf, ThriftReadSupport.class);
    ThriftReadSupport.setRecordConverterClass(jobConf, TBaseRecordConverter.class);

    if (thriftClass != null) {
      ParquetThriftInputFormat.setThriftClass(jobConf, thriftClass);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sinkConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {

    if (thriftClass == null) {
      throw new IllegalArgumentException("To use ParquetTBaseScheme as a sink, you must specify a thrift class in the constructor");
    }

    jobConf.setOutputFormat(DeprecatedParquetOutputFormat.class);
    DeprecatedParquetOutputFormat.setWriteSupportClass(jobConf, ThriftWriteSupport.class);
    ThriftWriteSupport.<T>setThriftClass(jobConf, thriftClass);
  }
}
