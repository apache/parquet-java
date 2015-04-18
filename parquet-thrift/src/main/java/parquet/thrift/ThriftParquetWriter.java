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

import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.thrift.TBaseWriteSupport;

/**
 * To generate Parquet files using thrift
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the thrift class used to write data
 */
public class ThriftParquetWriter<T extends TBase<?,?>> extends ParquetWriter<T> {

  /**
   * @deprecated use {@link #builder(Class<T>, Path)}
   */
  @Deprecated
  public ThriftParquetWriter(Path file, Class<T> thriftClass, CompressionCodecName compressionCodecName) throws IOException {
    super(file, new TBaseWriteSupport<T>(thriftClass), compressionCodecName, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
  }

  /**
   * @deprecated use {@link #builder(Class<T>, Path)}
   */
  @Deprecated
  public ThriftParquetWriter(Path file, Class<T> thriftClass, CompressionCodecName compressionCodecName, int blockSize, int pageSize, boolean enableDictionary, boolean validating) throws IOException {
    super(file, new TBaseWriteSupport<T>(thriftClass), compressionCodecName, blockSize, pageSize, enableDictionary, validating);
  }

  /**
   * @deprecated use {@link #builder(Class<T>, Path)}
   */
  @Deprecated
  public ThriftParquetWriter(Path file, Class<T> thriftClass, CompressionCodecName compressionCodecName, int blockSize, int pageSize, boolean enableDictionary, boolean validating, Configuration conf) throws IOException {
    super(file, new TBaseWriteSupport<T>(thriftClass), compressionCodecName,
        blockSize, pageSize, pageSize, enableDictionary, validating,
        DEFAULT_WRITER_VERSION, conf);
  }
  
  /**
   * Convenience method for getting a new builder for {@link ProtoParquetWriter}
   */
  public static <T extends TBase<?,?>> Builder<T> builder(Class<T> thriftClass, Path file) {
    return new Builder<T>(new TBaseWriteSupport<T>(thriftClass), file);
  }

}
