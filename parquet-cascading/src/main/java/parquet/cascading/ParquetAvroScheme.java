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

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import parquet.filter2.predicate.FilterPredicate;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.mapred.Container;

import static parquet.Preconditions.checkNotNull;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.generic.GenericData.Record;
import parquet.avro.AvroReadSupport;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import cascading.avro.CascadingToAvro;

/**
 * A Cascading Scheme that returns a simple Tuple with a single value, the "value" object
 * coming out of the underlying Avro InputFormat.
 */
public class ParquetAvroScheme<T> extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

  public static final class Config<T> implements Serializable {
    private final FilterPredicate filterPredicate;
    private final String projectionString;
    private final Class<T> klass;

    private Config(Class<T> klass, FilterPredicate filterPredicate, String projectionString) {
      this.filterPredicate = filterPredicate;
      this.projectionString = projectionString;
      this.klass = klass;
    }

    public Config() {
      filterPredicate = null;
      projectionString = null;
      klass = null;
    }

    public FilterPredicate getFilterPredicate() {
      return filterPredicate;
    }

    public String getProjectionString() {
      return projectionString;
    }

    public Class<T> getKlass() {
      return klass;
    }

    public Config<T> withFilterPredicate(FilterPredicate f) {
      return new Config<T>(this.klass, checkNotNull(f, "filterPredicate"), this.projectionString);
    }

    public Config<T> withProjectionString(String p) {
      return new Config<T>(this.klass, this.filterPredicate, checkNotNull(p, "projectionFilter"));
    }

    public Config<T> withRecordClass(Class<T> klass) {
      return new Config<T>(checkNotNull(klass, "recordClass"), this.filterPredicate, this.projectionString);
    }
  }

  private static final long serialVersionUID = 157560846420730044L;
  protected final Config<T> config;
  protected transient Schema schema;
  protected String schemaStr;

  public ParquetAvroScheme() {
    this(new Config<T>());
  }

  public ParquetAvroScheme(FilterPredicate filterPredicate) {
    this(new Config<T>().withFilterPredicate(filterPredicate));
  }

  public ParquetAvroScheme(Config<T> config) {
    this.config = config;
  }

  public ParquetAvroScheme(String avroSchema) {
    this(new Config<T>());
    schemaStr = avroSchema;
  }

  private void setProjectionPushdown(JobConf jobConf) {
    if (this.config.projectionString!= null) {
      AvroReadSupport.setRequestedProjection(jobConf, new Schema.Parser().parse(this.config.projectionString));
    }
  }

  private void setPredicatePushdown(JobConf jobConf) {
    if (this.config.filterPredicate != null) {
      ParquetInputFormat.setFilterPredicate(jobConf, this.config.filterPredicate);
    }
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> jobConfFlowProcess, Tap<JobConf, RecordReader, OutputCollector> jobConfRecordReaderOutputCollectorTap, final JobConf jobConf) {
    setPredicatePushdown(jobConf);
    setProjectionPushdown(jobConf);
    setRecordClass(jobConf);
  }

  private void setRecordClass(JobConf jobConf) {
    jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean source(FlowProcess<JobConf> fp, SourceCall<Object[], RecordReader> sc)
          throws IOException {
    Container<T> value = (Container<T>) sc.getInput().createValue();
    boolean hasNext = sc.getInput().next(null, value);
    if (!hasNext) { return false; }

    // Skip nulls
    if (value == null) { return true; }

    sc.getIncomingEntry().setTuple(new Tuple(value.get()));
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void sink(FlowProcess<JobConf> fp, SinkCall<Object[], OutputCollector> sc)
          throws IOException {
    TupleEntry tuple = sc.getOutgoingEntry();

    IndexedRecord record = new Record(schema);
    Object[] objectArray = CascadingToAvro.parseTupleEntry(tuple, schema);
    for (int i = 0; i < objectArray.length; i++) {
      record.put(i, objectArray[i]);
    }
    // noinspection unchecked
    sc.getOutput().collect(null, record);

  }

  @Override
  // Method sinkInit initializes this instance as a sink.
  public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    conf.setOutputFormat(DeprecatedParquetOutputFormat.class);
    conf.set("parquet.avro.schema", schemaStr);
    DeprecatedParquetOutputFormat.setWriteSupportClass(conf, AvroWriteSupport.class);
  }

  @Override
  public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    schema = new Schema.Parser().parse(schemaStr);
  }

}
