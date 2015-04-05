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
package parquet.cascading;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import parquet.avro.AvroReadSupport;
import parquet.avro.AvroWriteSupport;
import parquet.avro.projection.AvroProjection;
import parquet.filter2.predicate.FilterPredicate;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import parquet.hadoop.mapred.DeprecatedParquetOutputFormat;

public class ParquetAvroScheme<T extends IndexedRecord> extends ParquetValueScheme<T> {

  // In the case of reads, we can read the thrift class from the file metadata
  public ParquetAvroScheme() {
    this(new parquet.cascading.ParquetValueScheme.Config<T>());
  }

  public ParquetAvroScheme(Class<T> avroClass) {
    this(new parquet.cascading.ParquetValueScheme.Config<T>().withRecordClass(avroClass));
  }

  public ParquetAvroScheme(FilterPredicate filterPredicate) {
    this(new parquet.cascading.ParquetValueScheme.Config<T>().withFilterPredicate(filterPredicate));
  }

  public ParquetAvroScheme(FilterPredicate filterPredicate, Class<T> avroClass) {
    this(new parquet.cascading.ParquetValueScheme.Config<T>().withRecordClass(avroClass).withFilterPredicate(filterPredicate));
  }

  public ParquetAvroScheme(parquet.cascading.ParquetValueScheme.Config<T> config) {
    super(config);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sourceConfInit(FlowProcess<JobConf> fp,
                             Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    super.sourceConfInit(fp, tap, jobConf);
    try {
      jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
      ParquetInputFormat.setReadSupportClass(jobConf, AvroReadSupport.class);
    } catch (Exception e) {
      System.err.println(e.toString());
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sinkConfInit(FlowProcess<JobConf> fp,
                           Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    DeprecatedParquetOutputFormat.setAsOutputFormat(jobConf);
    DeprecatedParquetOutputFormat.setWriteSupportClass(jobConf, AvroWriteSupport.class);
    IndexedRecord record = createRecordInstance();
    AvroWriteSupport.setSchema(jobConf, record.getSchema());
  }

  @Override
  protected void setRecordClass(JobConf jobConf) {
    IndexedRecord record = createRecordInstance();
    AvroReadSupport.setAvroReadSchema(jobConf, record.getSchema());
  }

  @Override
  protected void setProjectionPushdown(JobConf jobConf) {
    if (this.config.getProjectionString() != null) {
      IndexedRecord record = createRecordInstance();
      AvroReadSupport.setRequestedProjection(
          jobConf,
          AvroProjection.createProjection(record.getSchema(), config.getProjectionString().split(";")));
    }
  }

  private IndexedRecord createRecordInstance() {
    if (config.getClass() == null) {
      throw new IllegalArgumentException("Unable to create instance of unknown Avro record class");
    }
    try {
      IndexedRecord record = config.getKlass().getConstructor(null).newInstance(null);
      return record;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Unable to create Avro Record of class " + config.getKlass().getName(), e);
    }
  }
}

