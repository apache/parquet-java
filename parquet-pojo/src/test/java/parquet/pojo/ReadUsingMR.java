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
package parquet.pojo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Reads data from given parquet file using MapReduce job.
 */
public class ReadUsingMR {

  private static List outputMessages;

  Configuration conf = new Configuration();
  private String projection;

  public void setRequestedProjection(String projection) {
    this.projection = projection;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public static class ReadingMapper extends Mapper<Void, Object, LongWritable, Object> {
    protected void map(Void key, Object value, Context context) throws IOException, InterruptedException {
      outputMessages.add(value);
    }
  }

  public List read(Path parquetPath, Class schemaType, Class... genericArguments) throws Exception {

    synchronized (ReadUsingMR.class) {
      outputMessages = new ArrayList();

      final Job job = new Job(conf, "read");
      job.setInputFormatClass(ParquetPojoInputFormat.class);

      ParquetPojoInputFormat.setInputPaths(job, parquetPath);
      if (genericArguments.length == 2) {
        ParquetPojoInputFormat.setMapSchemaClass(job, schemaType, genericArguments[0], genericArguments[1]);
        ParquetPojoOutputFormat.setMapSchemaClass(job, schemaType, genericArguments[0], genericArguments[1]);
      } else if (genericArguments.length == 1) {
        ParquetPojoInputFormat.setListSchemaClass(job, schemaType, genericArguments[0]);
        ParquetPojoOutputFormat.setListSchemaClass(job, schemaType, genericArguments[0]);
      } else {
        ParquetPojoInputFormat.setSchemaClass(job, schemaType);
        ParquetPojoOutputFormat.setSchemaClass(job, schemaType);
      }
      job.setMapperClass(ReadingMapper.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(NullOutputFormat.class);

      WriteUsingMR.waitForJob(job);

      List result = Collections.unmodifiableList(outputMessages);
      outputMessages = null;
      return result;
    }
  }

}
