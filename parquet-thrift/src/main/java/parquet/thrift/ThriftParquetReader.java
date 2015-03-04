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
package parquet.thrift;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;

import parquet.filter2.compat.FilterCompat;
import parquet.filter2.compat.FilterCompat.Filter;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.thrift.ThriftReadSupport;

import static parquet.Preconditions.checkNotNull;

/**
 * To read a parquet file into thrift objects
 * @author Julien Le Dem
 * @param <T> the thrift type
 */
public class ThriftParquetReader<T extends TBase<?,?>> extends ParquetReader<T> {

  /**
   * @param file the file to read
   * @param thriftClass the class used to read
   * @throws IOException
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
   * @throws IOException
   * @deprecated use {@link #build(Path)}
   */
  @Deprecated
  public ThriftParquetReader(Configuration conf, Path file, Class<T> thriftClass) throws IOException {
    super(conf, file, new ThriftReadSupport<T>(thriftClass));
  }

  /**
   * will use the thrift class based on the file metadata if a thrift class information is present
   * @param file the file to read
   * @throws IOException
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
   * @throws IOException
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
      this.file = checkNotNull(file, "file");
      this.conf = new Configuration();
      this.filter = FilterCompat.NOOP;
      this.thriftClass = null;
    }

    public Builder<T> withConf(Configuration conf) {
      this.conf = checkNotNull(conf, "conf");
      return this;
    }

    public Builder<T> withFilter(Filter filter) {
      this.filter = checkNotNull(filter, "filter");
      return this;
    }

    /**
     * If this is called, the thrift class is used.
     * If not, will use the thrift class based on the file
     * metadata if a thrift class information is present.
     */
    public Builder<T> withThriftClass(Class<T> thriftClass) {
      this.thriftClass = checkNotNull(thriftClass, "thriftClass");
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
