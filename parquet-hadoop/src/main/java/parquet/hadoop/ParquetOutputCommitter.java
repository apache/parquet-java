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
package parquet.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import parquet.Log;
import parquet.hadoop.util.ContextUtil;

public class ParquetOutputCommitter extends FileOutputCommitter {
  private static final Log LOG = Log.getLog(ParquetOutputCommitter.class);

  private final Path outputPath;

  public ParquetOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.outputPath = outputPath;
  }

  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
    Configuration configuration = ContextUtil.getConfiguration(jobContext);
    if (configuration.getBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, true)) {
      try {
        final FileSystem fileSystem = outputPath.getFileSystem(configuration);
        FileStatus outputStatus = fileSystem.getFileStatus(outputPath);
        List<Footer> footers = ParquetFileReader.readAllFootersInParallel(configuration, outputStatus);
        try {
          ParquetFileWriter.writeMetadataFile(configuration, outputPath, footers);
        } catch (Exception e) {
          LOG.warn("could not write summary file for " + outputPath, e);
          final Path metadataPath = new Path(outputPath, ParquetFileWriter.PARQUET_METADATA_FILE);
          if (fileSystem.exists(metadataPath)) {
            fileSystem.delete(metadataPath, true);
          }
        }
      } catch (Exception e) {
        LOG.warn("could not write summary file for " + outputPath, e);
      }
    }
  }

}
