package parquet.cascading;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.google.protobuf.Message;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import parquet.proto.ProtoReadSupport;
import parquet.proto.ProtoWriteSupport;

public class ParquetProtoScheme<T extends Message> extends ParquetValueScheme<T> {

  private Class<T> protoClass;

  // In the case of reads, we can read the proto class from the file metadata
  public ParquetProtoScheme() {
  }

  public ParquetProtoScheme(Class<T> protoClass) {
    this(protoClass, Fields.ALL);
  }

  public ParquetProtoScheme(Class<T> protoClass, Fields fields) {
    this.protoClass = protoClass;
    super.setSinkFields(fields);
    super.setSourceFields(fields);
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
    ParquetInputFormat.setReadSupportClass(jobConf, ProtoReadSupport.class);
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {

    if (protoClass == null) {
      throw new IllegalArgumentException("To use " + this.getClass().getCanonicalName() + " as a sink, you must " +
              "specify a protobuf class in the constructor");
    }

    jobConf.setOutputFormat(DeprecatedParquetOutputFormat.class);
    DeprecatedParquetOutputFormat.setWriteSupportClass(jobConf, ProtoWriteSupport.class);
    ProtoWriteSupport.setSchema(jobConf, protoClass);
  }
}
