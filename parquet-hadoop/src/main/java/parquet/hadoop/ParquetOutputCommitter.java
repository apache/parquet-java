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
package parquet.hadoop;

import java.io.IOException;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import parquet.Log;

public class ParquetOutputCommitter extends FileOutputCommitter {
  private static final Log LOG = Log.getLog(ParquetOutputCommitter.class);

  private final Path outputPath;

  public ParquetOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.outputPath = outputPath;
  }

  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
    try {
      Configuration configuration = jobContext.getConfiguration();
      FileStatus outputStatus = outputPath.getFileSystem(configuration).getFileStatus(outputPath);
      List<Footer> footers = ParquetFileReader.readAllFootersInParallel(configuration, outputStatus);
      ParquetFileWriter.writeSummaryFile(configuration, outputPath, footers);
    } catch (Exception e) {
      LOG.warn("could not write summary file for " + outputPath, e);
    }
  }

}
