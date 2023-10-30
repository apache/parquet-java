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

import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

/**
 * Read Avro records from a Parquet file.
 *
 * @param <T> the Java type of records created by this reader
 */
public class AvroParquetReader<T> extends ParquetReader<T> {

  /**
   * @param file a file path
   * @param <T> the Java type of records to read from the file
   * @return an Avro reader builder
   * @deprecated will be removed in 2.0.0; use {@link #builder(InputFile)} instead.
   */
  @Deprecated
  public static <T> Builder<T> builder(Path file) {
    return new Builder<T>(file);
  }

  public static <T> Builder<T> builder(InputFile file) {
    return new Builder<T>(file);
  }

  public static <T> Builder<T> builder(InputFile file, ParquetConfiguration conf) {
    return new Builder<T>(file, conf);
  }

  /**
   * Convenience method for creating a ParquetReader which uses Avro
   * {@link GenericData} objects to store data from reads.
   *
   * @param file The location to read data from
   * @return A {@code ParquetReader} which reads data as Avro
   *         {@code GenericData}
   * @throws IOException if the InputFile has been closed, or if some other I/O
   *           error occurs
   */
  public static ParquetReader<GenericRecord> genericRecordReader(InputFile file) throws IOException {
    return new Builder<GenericRecord>(file).withDataModel(GenericData.get()).build();
  }

  /**
   * Convenience method for creating a ParquetReader which uses Avro
   * {@link GenericData} objects to store data from reads.
   *
   * @param file The location to read data from
   * @param conf The configuration to use
   * @return A {@code ParquetReader} which reads data as Avro
   *         {@code GenericData}
   * @throws IOException if the InputFile has been closed, or if some other I/O
   *           error occurs
   */
  public static ParquetReader<GenericRecord> genericRecordReader(InputFile file, ParquetConfiguration conf) throws IOException {
    return new Builder<GenericRecord>(file, conf).withDataModel(GenericData.get()).build();
  }

  /**
   * Convenience method for creating a ParquetReader which uses Avro
   * {@link GenericData} objects to store data from reads.
   *
   * @param file The location to read data from
   * @return A {@code ParquetReader} which reads data as Avro
   *         {@code GenericData}
   * @throws IOException if the InputFile has been closed, or if some other I/O
   *           error occurs
   *
   * @deprecated will be removed in 2.0.0; use {@link #genericRecordReader(InputFile)} instead.
   */
  @Deprecated
  public static ParquetReader<GenericRecord> genericRecordReader(Path file) throws IOException {
    return new Builder<GenericRecord>(file).withDataModel(GenericData.get()).build();
  }

  /**
   * @param file a file path
   * @throws IOException if there is an error while reading
   * @deprecated will be removed in 2.0.0; use {@link #builder(InputFile)} instead.
   */
  @Deprecated
  public AvroParquetReader(Path file) throws IOException {
    super(file, new AvroReadSupport<T>());
  }

  /**
   * @param file a file path
   * @param unboundRecordFilter an unbound record filter (from the old filter API)
   * @throws IOException if there is an error while reading
   * @deprecated will be removed in 2.0.0; use {@link #builder(InputFile)} instead.
   */
  @Deprecated
  public AvroParquetReader(Path file, UnboundRecordFilter unboundRecordFilter) throws IOException {
    super(file, new AvroReadSupport<T>(), unboundRecordFilter);
  }

  /**
   * @param conf a configuration
   * @param file a file path
   * @throws IOException if there is an error while reading
   * @deprecated will be removed in 2.0.0; use {@link #builder(InputFile)} instead.
   */
  @Deprecated
  public AvroParquetReader(Configuration conf, Path file) throws IOException {
    super(conf, file, new AvroReadSupport<T>());
  }

  /**
   * @param conf a configuration
   * @param file a file path
   * @param unboundRecordFilter an unbound record filter (from the old filter API)
   * @throws IOException if there is an error while reading
   * @deprecated will be removed in 2.0.0; use {@link #builder(InputFile)} instead.
   */
  @Deprecated
  public AvroParquetReader(Configuration conf, Path file, UnboundRecordFilter unboundRecordFilter) throws IOException {
    super(conf, file, new AvroReadSupport<T>(), unboundRecordFilter);
  }

  public static class Builder<T> extends ParquetReader.Builder<T> {

    private GenericData model = null;
    private boolean enableCompatibility = true;
    private boolean isReflect = true;

    @Deprecated
    private Builder(Path path) {
      super(path);
    }

    private Builder(InputFile file) {
      super(file);
    }

    private Builder(InputFile file, ParquetConfiguration conf) {
      super(file, conf);
    }

    public Builder<T> withDataModel(GenericData model) {
      this.model = model;

      // only generic and specific are supported by AvroIndexedRecordConverter
      if (model.getClass() != GenericData.class &&
          model.getClass() != SpecificData.class) {
        isReflect = true;
      }

      return this;
    }

    public Builder<T> disableCompatibility() {
      this.enableCompatibility = false;
      return this;
    }

    public Builder<T> withCompatibility(boolean enableCompatibility) {
      this.enableCompatibility = enableCompatibility;
      return this;
    }

    @Override
    protected ReadSupport<T> getReadSupport() {
      if (isReflect) {
        conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
      } else {
        conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, enableCompatibility);
      }
      return new AvroReadSupport<T>(model);
    }
  }
}
