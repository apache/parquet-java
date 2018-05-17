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
package parquet.hadoop.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import parquet.data.Group;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.util.ContextUtil;
import parquet.schema.MessageType;

/**
 * output format to write Parquet files in Parquet's native object model.
 *
 * must be provided the schema up front
 * @see GroupOutputFormat#setSchema(Configuration, MessageType)
 * @see GroupWriteSupport#PARQUET_SCHEMA
 *
 * @author Julien Le Dem
 *
 */
public class GroupOutputFormat extends ParquetOutputFormat<Group> {

  /**
   * set the schema being written to the job conf
   * @param schema the schema of the data
   * @param configuration the job configuration
   */
  public static void setSchema(Job job, MessageType schema) {
    GroupWriteSupport.setSchema(schema, ContextUtil.getConfiguration(job));
  }

  /**
   * retrieve the schema from the conf
   * @param configuration the job conf
   * @return the schema
   */
  public static MessageType getSchema(Job job) {
    return GroupWriteSupport.getSchema(ContextUtil.getConfiguration(job));
  }

  public GroupOutputFormat() {
    super(new GroupWriteSupport());
  }
}
