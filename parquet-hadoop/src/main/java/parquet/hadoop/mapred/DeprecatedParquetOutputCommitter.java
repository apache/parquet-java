package parquet.hadoop.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import parquet.hadoop.ParquetOutputCommitter;

import java.io.IOException;

/**
 *
 * Adapter for supporting ParquetOutputCommitter in mapred API
 *
 * @author Tianshuo Deng
 */
public class DeprecatedParquetOutputCommitter extends FileOutputCommitter {

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    JobConf jobConf = jobContext.getJobConf();
    Path outputPath = FileOutputFormat.getOutputPath(jobConf);
    ParquetOutputCommitter.writeMetaDataFile(jobConf, outputPath);
  }
}
