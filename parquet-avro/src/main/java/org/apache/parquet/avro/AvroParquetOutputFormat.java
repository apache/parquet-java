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
package org.apache.parquet.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;

/**
 * A Hadoop {@link org.apache.hadoop.mapreduce.OutputFormat} for Parquet files.
 */
public class AvroParquetOutputFormat<T> extends ParquetOutputFormat<T> {

  /**
   * Set the Avro schema to use for writing. The schema is translated into a
   * Parquet schema so that the records can be written in Parquet format. It is
   * also stored in the Parquet metadata so that records can be reconstructed as
   * Avro objects at read time without specifying a read schema.
   * 
   * @param job a job
   * @param schema a schema for the data that will be written
   * @see org.apache.parquet.avro.AvroParquetInputFormat#setAvroReadSchema(org.apache.hadoop.mapreduce.Job,
   * org.apache.avro.Schema)
   */
  public static void setSchema(Job job, Schema schema) {
    AvroWriteSupport.setSchema(ContextUtil.getConfiguration(job), schema);
  }

  public AvroParquetOutputFormat() {
    super(new AvroWriteSupport<T>());
  }

  /**
   * Sets the {@link AvroDataSupplier} class that will be used. The data supplier
   * provides instances of {@link org.apache.avro.generic.GenericData} that are
   * used to deconstruct records.
   *
   * @param job a {@link Job} to configure
   * @param supplierClass a supplier class
   */
  public static void setAvroDataSupplier(Job job, Class<? extends AvroDataSupplier> supplierClass) {
    AvroWriteSupport.setAvroDataSupplier(ContextUtil.getConfiguration(job), supplierClass);
  }
}
