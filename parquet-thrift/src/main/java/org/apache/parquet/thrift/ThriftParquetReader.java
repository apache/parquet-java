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
package org.apache.parquet.thrift;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.thrift.ThriftReadSupport;

/**
 * To read a parquet file into thrift objects
 * @param <T> the thrift type
 */
@Deprecated
public class ThriftParquetReader<T extends TBase<?,?>> extends ParquetReader<T> {

  /**
   * @param file the file to read
   * @param thriftClass the class used to read
   * @throws IOException if there is an error while reading
   * @deprecated use {@link #build(Path)}
   */
  @Deprecated
  public ThriftParquetReader(Path file, Class<T> thriftClass) throws IOException {
    super(file, new ThriftReadSupport<T>(thriftClass));
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param thriftClass the class used to read
   * @throws IOException if there is an error while reading
   * @deprecated use {@link #build(Path)}
   */
  @Deprecated
  public ThriftParquetReader(Configuration conf, Path file, Class<T> thriftClass) throws IOException {
    super(conf, file, new ThriftReadSupport<T>(thriftClass));
  }

  /**
   * will use the thrift class based on the file metadata if a thrift class information is present
   * @param file the file to read
   * @throws IOException if there is an error while reading
   * @deprecated use {@link #build(Path)}
   */
  @Deprecated
  public ThriftParquetReader(Path file) throws IOException {
    super(file, new ThriftReadSupport<T>());
  }

  /**
   * will use the thrift class based on the file metadata if a thrift class information is present
   * @param conf the configuration
   * @param file the file to read
   * @throws IOException if there is an error while reading
   * @deprecated use {@link #build(Path)}
   */
  @Deprecated
  public ThriftParquetReader(Configuration conf, Path file) throws IOException {
    super(conf, file, new ThriftReadSupport<T>());
  }

  public static <T extends TBase<?,?>> Builder<T> build(Path file) {
    return new Builder<T>(file);
  }

  public static class Builder<T extends TBase<?,?>> {
    private final Path file;
    private Configuration conf;
    private Filter filter;
    private Class<T> thriftClass;

    private Builder(Path file) {
      this.file = Objects.requireNonNull(file, "file cannot be null");
      this.conf = new Configuration();
      this.filter = FilterCompat.NOOP;
      this.thriftClass = null;
    }

    public Builder<T> withConf(Configuration conf) {
      this.conf = Objects.requireNonNull(conf, "conf cannot be null");
      return this;
    }

    public Builder<T> withFilter(Filter filter) {
      this.filter = Objects.requireNonNull(filter, "filter cannot be null");
      return this;
    }

    /**
     * If this is called, the thrift class is used.
     * If not, will use the thrift class based on the file
     * metadata if a thrift class information is present.
     *
     * @param thriftClass a thrift class
     * @return this for method chaining
     */
    public Builder<T> withThriftClass(Class<T> thriftClass) {
      this.thriftClass = Objects.requireNonNull(thriftClass, "thriftClass cannot be null");
      return this;
    }

    public ParquetReader<T> build() throws IOException {
      ReadSupport<T> readSupport;

      if (thriftClass != null) {
        readSupport = new ThriftReadSupport<T>(thriftClass);
      } else {
        readSupport = new ThriftReadSupport<T>();
      }

      return ParquetReader.builder(readSupport, file).withConf(conf).withFilter(filter).build();
    }
  }

}
