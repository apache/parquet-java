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
package parquet.thrift;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;

import parquet.hadoop.ParquetReader;
import parquet.hadoop.thrift.ThriftReadSupport;

/**
 * To read a parquet file into thrift objects
 * @author Julien Le Dem
 *
 * @param <T> the thrift type
 */
public class ThriftParquetReader<T extends TBase<?,?>> extends ParquetReader<T> {

  /**
   * @param file the file to read
   * @param thriftClass the class used to read
   * @throws IOException
   */
  public ThriftParquetReader(Path file, Class<T> thriftClass) throws IOException {
    super(file, new ThriftReadSupport<T>(thriftClass));
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param thriftClass the class used to read
   * @throws IOException
   */
  public ThriftParquetReader(Configuration conf, Path file, Class<T> thriftClass) throws IOException {
    super(conf, file, new ThriftReadSupport<T>(thriftClass));
  }

  /**
   * will use the thrift class based on the file metadata if a thrift class information is present
   * @param file the file to read
   * @throws IOException
   */
  public ThriftParquetReader(Path file) throws IOException {
    super(file, new ThriftReadSupport<T>());
  }

  /**
   * will use the thrift class based on the file metadata if a thrift class information is present
   * @param conf the configuration
   * @param file the file to read
   * @throws IOException
   */
  public ThriftParquetReader(Configuration conf, Path file) throws IOException {
    super(conf, file, new ThriftReadSupport<T>());
  }

}
