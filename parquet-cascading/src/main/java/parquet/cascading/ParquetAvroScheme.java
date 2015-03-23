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
import parquet.hadoop.metadata.CompressionCodecName;

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
        IndexedRecord record = (IndexedRecord)config.getKlass().getConstructor(null).newInstance(null);
        ParquetInputFormat.setReadSupportClass(jobConf, AvroReadSupport.class);
        AvroReadSupport.setAvroReadSchema(jobConf, record.getSchema());
        AvroReadSupport.setRequestedProjection(jobConf, AvroProjection.createProjection(record.getSchema(), config.getProjectionString().split(";")));
        ParquetInputFormat.setFilterPredicate(jobConf, config.getFilterPredicate());
    } catch (Exception e) {
        System.err.println(e.toString());
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sinkConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {

      if (this.config.getKlass() == null) {
          throw new IllegalArgumentException("To use ParquetAvroScheme as a sink, you must specify an avro class in the constructor");
      }

      try {
          jobConf.setOutputFormat(DeprecatedParquetOutputFormat.class);
          DeprecatedParquetOutputFormat.setWriteSupportClass(jobConf, AvroWriteSupport.class);
          DeprecatedParquetOutputFormat.setCompressOutput(jobConf, true);
          DeprecatedParquetOutputFormat.setCompression(jobConf, CompressionCodecName.SNAPPY);
          IndexedRecord record = (IndexedRecord)config.getKlass().getConstructor(null).newInstance(null);
          AvroWriteSupport.setSchema(jobConf, record.getSchema());
      } catch (Exception e) {
          System.err.println(e.toString());
      }
  }
}

