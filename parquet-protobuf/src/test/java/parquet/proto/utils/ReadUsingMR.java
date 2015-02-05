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
package parquet.proto.utils;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import parquet.proto.ProtoParquetInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Reads data from given parquet file using MapReduce job.
 */
public class ReadUsingMR {

  private static List<Message> outputMessages;

  Configuration conf = new Configuration();
  private String projection;

  public void setRequestedProjection(String projection) {
    this.projection = projection;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public static class ReadingMapper extends Mapper<Void, MessageOrBuilder, LongWritable, Message> {
    protected void map(Void key, MessageOrBuilder value, Context context) throws IOException, InterruptedException {
      Message clone = ((Message.Builder) value).build();
      outputMessages.add(clone);
    }
  }

  public List<Message> read(Path parquetPath) throws Exception {

    synchronized (ReadUsingMR.class) {
      outputMessages = new ArrayList<Message>();

      final Job job = new Job(conf, "read");
      job.setInputFormatClass(ProtoParquetInputFormat.class);
      ProtoParquetInputFormat.setInputPaths(job, parquetPath);
      if (projection != null) {
        ProtoParquetInputFormat.setRequestedProjection(job, projection);
      }

      job.setMapperClass(ReadingMapper.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(NullOutputFormat.class);

      WriteUsingMR.waitForJob(job);

      List<Message> result = Collections.unmodifiableList(outputMessages);
      outputMessages = null;
      return result;
    }
  }

}
