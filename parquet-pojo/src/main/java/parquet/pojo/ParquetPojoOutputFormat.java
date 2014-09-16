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

import org.apache.hadoop.mapreduce.Job;
import parquet.hadoop.ParquetOutputFormat;

import java.util.List;
import java.util.Map;

/**
 * A {@link ParquetOutputFormat} for writing native java objects to parquet files
 * <p/>
 * Usage:
 * <p/>
 * <pre>
 * {@code
 * final Job job = new Job(conf, "Parquet writing job");
 * job.setOutputFormatClass(ParquetPojoOutputFormat.class);
 * ParquetPojoOutputFormat.setOutputPath(job, parquetPath);
 * ParquetPojoOutputFormat.setSchemaClass(job, YourPojo.class);
 * }
 * </pre>
 *
 */
public class ParquetPojoOutputFormat extends ParquetOutputFormat {
  public ParquetPojoOutputFormat() {
    super(new PojoWriteSupport());
  }

  /**
   * Encodes the class to be used in writing into the job conf, this is then written into the file header
   *
   * @param job   configuration for the job
   * @param clazz the class who's name will be serialized
   */
  public static void setSchemaClass(Job job, Class clazz) {
    SchemaUtils.setSchemaClass(PojoType.Output, job, clazz);
  }

  public static void setMapSchemaClass(Job job, Class<? extends Map> clazz, Class keyClass, Class valueClass) {
    SchemaUtils.setMapSchemaClass(PojoType.Output, job, clazz, keyClass, valueClass);
  }

  public static void setListSchemaClass(Job job, Class<? extends List> clazz, Class valueClass) {
    SchemaUtils.setListSchemaClass(PojoType.Output, job, clazz, valueClass);
  }
}
