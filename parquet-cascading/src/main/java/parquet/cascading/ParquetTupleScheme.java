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

 import java.io.IOException;
 import java.util.List;
 import org.apache.hadoop.mapred.JobConf;
 import org.apache.hadoop.mapred.OutputCollector;
 import org.apache.hadoop.mapred.RecordReader;
 import org.apache.hadoop.fs.Path;

 import parquet.hadoop.ParquetInputFormat;
 import parquet.hadoop.ParquetFileReader;
 import parquet.hadoop.Footer;
 import parquet.hadoop.metadata.ParquetMetadata;
 import parquet.hadoop.mapred.Container;
 import parquet.hadoop.mapred.DeprecatedParquetInputFormat;
 import parquet.schema.MessageType;

 import cascading.flow.FlowProcess;
 import cascading.scheme.SinkCall;
 import cascading.scheme.Scheme;
 import cascading.scheme.SourceCall;
 import cascading.tap.Tap;
 import cascading.tap.TapException;
 import cascading.tap.CompositeTap;
 import cascading.tap.hadoop.Hfs;
 import cascading.tuple.Tuple;
 import cascading.tuple.Fields;

 /**
  * A Cascading Scheme that converts Parquet groups into Cascading tuples.
  * If you provide it with sourceFields, it will selectively materialize only the columns for those fields.
  * The names must match the names in the Parquet schema.
  * If you do not provide sourceFields, or use Fields.ALL or Fields.UNKNOWN, it will create one from the
  * Parquet schema.
  * Currently, only primitive types are supported. TODO: allow nested fields in the Parquet schema to be
  * flattened to a top-level field in the Cascading tuple.
  *
  * @author Avi Bryant
  */

public class ParquetTupleScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]>{

  private static final long serialVersionUID = 0L;

  public ParquetTupleScheme() {
    super();
  }

  public ParquetTupleScheme(Fields sourceFields) {
    super(sourceFields);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sourceConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
    ParquetInputFormat.setReadSupportClass(jobConf, TupleReadSupport.class);
    TupleReadSupport.setRequestedFields(jobConf, getSourceFields());
 }

 @Override
 public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
    MessageType schema = readSchema(flowProcess, tap);
    SchemaIntersection intersection = new SchemaIntersection(schema, getSourceFields());

    setSourceFields(intersection.getSourceFields());

    return getSourceFields();
  }

  private MessageType readSchema(FlowProcess<JobConf> flowProcess, Tap tap) {
    try {
      Hfs hfs;

      if( tap instanceof CompositeTap )
        hfs = (Hfs) ( (CompositeTap) tap ).getChildTaps().next();
      else
        hfs = (Hfs) tap;

      JobConf jobConf = flowProcess.getConfigCopy();
      DeprecatedParquetInputFormat format = new DeprecatedParquetInputFormat();
      format.addInputPath(jobConf, hfs.getPath());
      List<Footer> footers = format.getFooters(jobConf);

      if(footers.isEmpty()) {
        throw new TapException("Could not read Parquet metadata at " + hfs.getPath());
      } else {
        return footers.get(0).getParquetMetadata().getFileMetaData().getSchema();
      }
    } catch (IOException e) {
      throw new TapException(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean source(FlowProcess<JobConf> fp, SourceCall<Object[], RecordReader> sc)
      throws IOException {
    Container<Tuple> value = (Container<Tuple>) sc.getInput().createValue();
    boolean hasNext = sc.getInput().next(null, value);
    if (!hasNext) { return false; }

    // Skip nulls
    if (value == null) { return true; }

    sc.getIncomingEntry().setTuple(value.get());
    return true;
  }


  @SuppressWarnings("rawtypes")
  @Override
  public void sinkConfInit(FlowProcess<JobConf> arg0,
      Tap<JobConf, RecordReader, OutputCollector> arg1, JobConf arg2) {
    throw new UnsupportedOperationException("ParquetTupleScheme does not support Sinks");

  }

  @Override
  public boolean isSink() { return false; }


  @Override
  public void sink(FlowProcess<JobConf> arg0, SinkCall<Object[], OutputCollector> arg1)
      throws IOException {
    throw new UnsupportedOperationException("ParquetTupleScheme does not support Sinks");
  }
}