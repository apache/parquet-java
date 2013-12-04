package parquet.hadoop.codec;


import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.junit.Test;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class MapReduceCodecConfigTest {
  @Test
  public void all() throws IOException {
    for (CompressionCodecName codec : CompressionCodecName.values()) {
      shouldUseParquetFlagToSetCodec(codec.name(), codec);
      shouldUseHadoopFlagToSetCodec(codec.getHadoopCompressionCodecClassName(), codec);
    }
    shouldUseHadoopFlagToSetCodec("unexistedCodec", CompressionCodecName.UNCOMPRESSED);
    shouldUseHadoopFlagToSetCodec("org.apache.hadoop.io.compress.DefaultCodec", CompressionCodecName.UNCOMPRESSED);
  }

  public void shouldUseParquetFlagToSetCodec(String codecNameStr, CompressionCodecName expectedCodec) throws IOException {
    Job job = new Job();
    Configuration conf = job.getConfiguration();
    conf.set(ParquetOutputFormat.COMPRESSION, codecNameStr);
    TaskAttemptContext task = new TaskAttemptContext(conf, new TaskAttemptID(new TaskID(new JobID("test", 1), false, 1), 1));
    Assert.assertEquals(new MapReduceCodecConfig(task).getCodec(), expectedCodec);
  }

  public void shouldUseHadoopFlagToSetCodec(String codecClassStr, CompressionCodecName expectedCodec) throws IOException {

    Job job = new Job();
    Configuration conf = job.getConfiguration();
    conf.setBoolean("mapred.output.compress", true);
    if (codecClassStr != null) {
      conf.set("mapred.output.compression.codec", codecClassStr);
    }
    TaskAttemptContext task = new TaskAttemptContext(conf, new TaskAttemptID(new TaskID(new JobID("test", 1), false, 1), 1));
    Assert.assertEquals(expectedCodec, new MapReduceCodecConfig(task).getCodec());

  }


}
