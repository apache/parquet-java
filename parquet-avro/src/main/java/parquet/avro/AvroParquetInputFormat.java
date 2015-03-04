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
package parquet.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.mapreduce.Job;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.util.ContextUtil;

/**
 * A Hadoop {@link org.apache.hadoop.mapreduce.InputFormat} for Parquet files.
 */
public class AvroParquetInputFormat<T> extends ParquetInputFormat<T> {
  public AvroParquetInputFormat() {
    super(AvroReadSupport.class);
  }

  /**
   * Set the subset of columns to read (projection pushdown). Specified as an Avro
   * schema, the requested projection is converted into a Parquet schema for Parquet
   * column projection.
   * <p>
   * This is useful if the full schema is large and you only want to read a few
   * columns, since it saves time by not reading unused columns.
   * <p>
   * If a requested projection is set, then the Avro schema used for reading
   * must be compatible with the projection. For instance, if a column is not included
   * in the projection then it must either not be included or be optional in the read
   * schema. Use {@link #setAvroReadSchema(org.apache.hadoop.mapreduce.Job,
   * org.apache.avro.Schema)} to set a read schema, if needed.
   * @param job
   * @param requestedProjection
   * @see #setAvroReadSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   * @see parquet.avro.AvroParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  public static void setRequestedProjection(Job job, Schema requestedProjection) {
    AvroReadSupport.setRequestedProjection(ContextUtil.getConfiguration(job),
        requestedProjection);
  }

  /**
   * Override the Avro schema to use for reading. If not set, the Avro schema used for
   * writing is used.
   * <p>
   * Differences between the read and write schemas are resolved using
   * <a href="http://avro.apache.org/docs/current/spec.html#Schema+Resolution">Avro's schema resolution rules</a>.
   * @param job
   * @param avroReadSchema
   * @see #setRequestedProjection(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   * @see parquet.avro.AvroParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  public static void setAvroReadSchema(Job job, Schema avroReadSchema) {
    AvroReadSupport.setAvroReadSchema(ContextUtil.getConfiguration(job), avroReadSchema);
  }

  /**
   * Uses an instance of the specified {@link AvroDataSupplier} class to control how the
   * {@link org.apache.avro.specific.SpecificData} instance that is used to find
   * Avro specific records is created.
   * @param job
   * @param supplierClass
   */
  public static void setAvroDataSupplier(Job job,
      Class<? extends AvroDataSupplier> supplierClass) {
    AvroReadSupport.setAvroDataSupplier(ContextUtil.getConfiguration(job), supplierClass);
  }
}
