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
 import cascading.tuple.Tuple;
 import cascading.tuple.Fields;


public class ParquetTupleScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]>{

  private static final long serialVersionUID = 0L;

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

    if( tap instanceof CompositeTap )
      tap = (Tap) ( (CompositeTap) tap ).getChildTaps().next();

    MessageType schema = readSchema(flowProcess, new Path(tap.getFullIdentifier(flowProcess.getConfigCopy())));

    setSourceFields(FilterSchema.filterFields(schema, getSourceFields()));

    return getSourceFields();
  }

  private MessageType readSchema(FlowProcess<JobConf> flowProcess, Path path) {
    ParquetMetadata metadata = ParquetFileReader.readAnyFooter(flowProcess.getConfigCopy(), path);
    return metadata.getFileMetaData().getSchema();
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