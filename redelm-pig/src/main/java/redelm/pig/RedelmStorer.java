package redelm.pig;

import java.io.IOException;
import java.util.Properties;

import redelm.schema.MessageType;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

public class RedelmStorer extends StoreFunc implements StoreMetadata {

  private static final String SCHEMA = "schema";

  private RecordWriter<Object, Tuple> recordWriter;
  private String location;


  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    UDFContext udfc = UDFContext.getUDFContext();
    Properties p =
        udfc.getUDFProperties(this.getClass(), new String[]{});
    p.setProperty(SCHEMA, s.toString());
  }

  @Override
  public OutputFormat<Object, Tuple> getOutputFormat() throws IOException {
    MessageType schema = new PigSchemaConverter().convert(getSchema());

    return new RedelmOutputFormat(schema);
  }

  private Schema getSchema() {
    UDFContext udfc = UDFContext.getUDFContext();
    Properties p = udfc.getUDFProperties(this.getClass(), new String[]{});
    try {
      return Utils.getSchemaFromString(p.getProperty(SCHEMA));
    } catch (ParserException e) {
      throw new RuntimeException("can not get schema from context", e);
    }
  }

  @Override
  public void prepareToWrite(RecordWriter recordWriter) throws IOException {
    this.recordWriter = recordWriter;
  }

  @Override
  public void putNext(Tuple tuple) throws IOException {
    try {
      this.recordWriter.write(null, tuple);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new RuntimeException("Interrupted while writing", e);
    }
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    this.location = location;
    FileOutputFormat.setOutputPath(job, new Path(location));
  }

  @Override
  public void storeSchema(ResourceSchema schema, String location, Job job)
      throws IOException {
  }

  @Override
  public void storeStatistics(ResourceStatistics resourceStatistics, String location, Job job)
      throws IOException {
  }

}
