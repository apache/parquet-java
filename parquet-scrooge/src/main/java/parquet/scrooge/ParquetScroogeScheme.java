package parquet.scrooge;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.scrooge.ThriftStruct;

import parquet.cascading.ParquetValueScheme;
import parquet.hadoop.thrift.ParquetThriftInputFormat;
import parquet.hadoop.thrift.ThriftReadSupport;
import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.tap.Tap;

public class ParquetScroogeScheme<T extends ThriftStruct> extends ParquetValueScheme<T> {

  private static final long serialVersionUID = -8332274507341448397L;
  private Class<T> klass;

  public ParquetScroogeScheme(Class<T> klass) {
    this.klass = klass;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sinkConfInit(FlowProcess<JobConf> arg0,
      Tap<JobConf, RecordReader, OutputCollector> arg1, JobConf arg2) {
    throw new RuntimeException("ParquetScroogeScheme does not support Sinks");

  }


  /**
   * TODO: currently we cannot write Parquet files from Scrooge objects.
   */
  @Override
  public boolean isSink() { return false; }


  @SuppressWarnings("rawtypes")
  @Override
  public void sourceConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    DeprecatedInputFormatWrapper.setInputFormat(ParquetThriftInputFormat.class, jobConf);
    ParquetThriftInputFormat.setReadSupportClass(jobConf, ThriftReadSupport.class);
    ThriftReadSupport.setRecordConverterClass(jobConf, ScroogeRecordConverter.class);
    ParquetThriftInputFormat.<T>setThriftClass(jobConf, klass);

  }

  @Override
  public void sink(FlowProcess<JobConf> arg0, SinkCall<Object[], OutputCollector> arg1)
      throws IOException {
    // TODO Auto-generated method stub

  }
}
