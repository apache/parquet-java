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
package parquet.proto;

import java.io.IOException;

import com.google.protobuf.MessageOrBuilder;

import org.apache.hadoop.fs.Path;

import parquet.filter.UnboundRecordFilter;
import parquet.hadoop.ParquetReader;

/**
 * Read Protobuf records from a Parquet file.
 */
public class ProtoParquetReader<T extends MessageOrBuilder> extends ParquetReader<T> {

  @SuppressWarnings("unchecked")
  public Builder<T> builder(Path file) {
    return ParquetReader.builder(new ProtoReadSupport(), file);
  }

  /**
   * @deprecated use {@link #builder(Path)}
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public ProtoParquetReader(Path file) throws IOException {
    super(file, new ProtoReadSupport());
  }

  /**
   * @deprecated use {@link #builder(Path)}
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public ProtoParquetReader(Path file, UnboundRecordFilter recordFilter) throws IOException {
    super(file, new ProtoReadSupport(), recordFilter);
  }
}
