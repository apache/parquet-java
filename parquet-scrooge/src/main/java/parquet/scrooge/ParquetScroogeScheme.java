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

package parquet.scrooge;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import com.twitter.scrooge.ThriftStruct;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.tap.Tap;
import parquet.cascading.ParquetValueScheme;
import parquet.filter2.predicate.FilterPredicate;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import parquet.hadoop.thrift.ParquetThriftInputFormat;
import parquet.hadoop.thrift.ThriftReadSupport;

public class ParquetScroogeScheme<T extends ThriftStruct> extends ParquetValueScheme<T> {

  private static final long serialVersionUID = -8332274507341448397L;
  private final Class<T> klass;

  public ParquetScroogeScheme(Class<T> klass) {
    this.klass = klass;
  }

  public ParquetScroogeScheme(FilterPredicate filterPredicate, Class<T> klass) {
    super(filterPredicate);
    this.klass = klass;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sinkConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    jobConf.setOutputFormat(DeprecatedParquetOutputFormat.class);
    ParquetOutputFormat.setWriteSupportClass(jobConf, ScroogeWriteSupport.class);
    ScroogeWriteSupport.setScroogeClass(jobConf, klass);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sourceConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
    ParquetInputFormat.setReadSupportClass(jobConf, ScroogeReadSupport.class);
    ThriftReadSupport.setRecordConverterClass(jobConf, ScroogeRecordConverter.class);
    ParquetThriftInputFormat.<T>setThriftClass(jobConf, klass);
  }
}
