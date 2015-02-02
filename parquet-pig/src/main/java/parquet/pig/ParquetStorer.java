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
package parquet.pig;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import parquet.hadoop.ParquetOutputFormat;
import parquet.io.ParquetEncodingException;

/**
 * A pig storer implementation for the Parquet file format.
 * see {@link ParquetOutputFormat} for available parameters.
 *
 * It uses a TupleWriteSupport to write Tuples into the ParquetOutputFormat
 * The Pig schema is automatically converted to the Parquet schema using {@link PigSchemaConverter}
 * and stored in the file
 *
 * @author Julien Le Dem
 *
 */
public class ParquetStorer extends StoreFunc implements StoreMetadata {

  private static final String SCHEMA = "schema";

  private RecordWriter<Void, Tuple> recordWriter;

  private String signature;

  private Properties getProperties() {
    UDFContext udfc = UDFContext.getUDFContext();
    Properties p =
        udfc.getUDFProperties(this.getClass(), new String[]{ signature });
    return p;
  }

  private Schema getSchema() {
    try {
      final String schemaString = getProperties().getProperty(SCHEMA);
      if (schemaString == null) {
        throw new ParquetEncodingException("Can not store relation in Parquet as the schema is unknown");
      }
      return Utils.getSchemaFromString(schemaString);
    } catch (ParserException e) {
      throw new ParquetEncodingException("can not get schema from context", e);
    }
  }

  public ParquetStorer() {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    super.setStoreFuncUDFContextSignature(signature);
    this.signature = signature;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    getProperties().setProperty(SCHEMA, s.toString());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OutputFormat<Void, Tuple> getOutputFormat() throws IOException {
    Schema pigSchema = getSchema();
    return new ParquetOutputFormat<Tuple>(new TupleWriteSupport(pigSchema));
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings({ "rawtypes", "unchecked" }) // that's how the base class is defined
  @Override
  public void prepareToWrite(RecordWriter recordWriter) throws IOException {
    this.recordWriter = recordWriter;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putNext(Tuple tuple) throws IOException {
    try {
      this.recordWriter.write(null, tuple);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new ParquetEncodingException("Interrupted while writing", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    FileOutputFormat.setOutputPath(job, new Path(location));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void storeSchema(ResourceSchema schema, String location, Job job)
      throws IOException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void storeStatistics(ResourceStatistics resourceStatistics, String location, Job job)
      throws IOException {
  }

}
