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
package org.apache.parquet.proto;

import java.io.IOException;

import com.google.protobuf.MessageOrBuilder;

import org.apache.hadoop.fs.Path;

import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoParquetWriter.Builder;

/**
 * Read Protobuf records from a Parquet file.
 */
public class ProtoParquetReader<T extends MessageOrBuilder> extends ParquetReader<T> {

  @SuppressWarnings("unchecked")
  public static <T extends MessageOrBuilder> Builder<T> builder(Path file) {
    return (Builder<T>) ParquetReader.builder(new ProtoReadSupport<>(), file);
  }

  @SuppressWarnings("unchecked")
  public static <T extends MessageOrBuilder> Builder<T> builder(Path file, T message) {
    return (Builder<T>) ParquetReader.builder(new ProtoReadSupport<>(), file);
    
  }

  /**
   * @param file a file path
   * @throws IOException if there is an error while reading
   * @deprecated use {@link #builder(Path)}
   */
  @Deprecated
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public ProtoParquetReader(Path file) throws IOException {
    super(file, new ProtoReadSupport());
  }

  /**
   * @param file a file path
   * @param recordFilter an unbound record filter
   * @throws IOException if there is an error while reading
   * @deprecated use {@link #builder(Path)}
   */
  @Deprecated
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public ProtoParquetReader(Path file, UnboundRecordFilter recordFilter) throws IOException {
    super(file, new ProtoReadSupport(), recordFilter);
  }
}
