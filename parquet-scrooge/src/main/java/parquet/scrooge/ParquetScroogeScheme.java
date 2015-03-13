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
package parquet.scrooge;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import com.twitter.scrooge.ThriftStruct;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import parquet.cascading.ParquetValueScheme;
import parquet.filter2.predicate.FilterPredicate;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import parquet.hadoop.thrift.ThriftReadSupport;

public class ParquetScroogeScheme<T extends ThriftStruct> extends ParquetValueScheme<T> {

  private static final long serialVersionUID = -8332274507341448397L;

  public ParquetScroogeScheme(Class<T> klass) {
    this(new Config<T>().withRecordClass(klass));
  }

  public ParquetScroogeScheme(FilterPredicate filterPredicate, Class<T> klass) {
    this(new Config<T>().withFilterPredicate(filterPredicate).withRecordClass(klass));
  }

  public ParquetScroogeScheme(Config<T> config) {
    super(config);
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    DeprecatedParquetOutputFormat.setAsOutputFormat(jobConf);
    ParquetOutputFormat.setWriteSupportClass(jobConf, ScroogeWriteSupport.class);
    ScroogeWriteSupport.setScroogeClass(jobConf, this.config.getKlass());
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    super.sourceConfInit(fp, tap, jobConf);
    jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
    ParquetInputFormat.setReadSupportClass(jobConf, ScroogeReadSupport.class);
    ThriftReadSupport.setRecordConverterClass(jobConf, ScroogeRecordConverter.class);
  }
}
