 package parquet.cascading;

 import java.io.IOException;

 import org.apache.hadoop.mapred.JobConf;
 import org.apache.hadoop.mapred.OutputCollector;
 import org.apache.hadoop.mapred.RecordReader;
 import org.apache.hadoop.fs.Path;

 import parquet.hadoop.ParquetInputFormat;
 import parquet.hadoop.ParquetFileReader;
 import parquet.hadoop.metadata.ParquetMetadata;
 import parquet.hadoop.thrift.Container;
 import parquet.hadoop.thrift.DeprecatedContainerInputFormat;
 import parquet.schema.MessageType;

 import cascading.flow.FlowProcess;
 import cascading.scheme.SinkCall;
 import cascading.scheme.Scheme;
 import cascading.scheme.SourceCall;
 import cascading.tap.Tap;
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
    DeprecatedContainerInputFormat.setInputFormat(ParquetInputFormat.class, jobConf);
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

      ParquetMetadata metadata = ParquetFileReader.readAnyFooter(flowProcess.getConfigCopy(), hfs.getPath());
      return metadata.getFileMetaData().getSchema();
    } catch (IOException e) {
      throw new RuntimeException(e.toString());
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