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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

import com.google.protobuf.MessageOrBuilder;

import static org.apache.parquet.proto.ProtoConstants.IGNORE_UNKNOWN_FIELDS;

/**
 * Read Protobuf records from a Parquet file.
 */
public class ProtoParquetReader<T extends MessageOrBuilder>
    extends ParquetReader<T> {

  public static <T> ParquetReader.Builder<T> builder(Path file) {
    return new ProtoParquetReader.Builder<T>(file);
  }

  public static <T> ParquetReader.Builder<T> builder(Path file, boolean ignoreUnknownFields) {
    return new ProtoParquetReader.Builder<T>(file).setIgnoreUnknownFields(ignoreUnknownFields);
  }

  public static <T> ParquetReader.Builder<T> builder(InputFile file) {
    return new ProtoParquetReader.Builder<T>(file);
  }

  public static <T> ParquetReader.Builder<T> builder(InputFile file, boolean ignoreUnknownFields) {
    return new ProtoParquetReader.Builder<T>(file).setIgnoreUnknownFields(ignoreUnknownFields);
  }

  /**
   * @param file a file path
   * @throws IOException if there is an error while reading
   * @deprecated use {@link #builder(Path)}
   */
  @Deprecated
  @SuppressWarnings("unchecked")
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
  @SuppressWarnings("unchecked")
  public ProtoParquetReader(Path file, UnboundRecordFilter recordFilter) throws IOException {
    super(file, new ProtoReadSupport(), recordFilter);
  }

  private static class Builder<T> extends ParquetReader.Builder<T> {

    protected Builder(InputFile file) {
      super(file);
    }

    protected Builder setIgnoreUnknownFields(boolean ignoreUnknownFields) {
      if(ignoreUnknownFields) {
        this.set(IGNORE_UNKNOWN_FIELDS, "TRUE");
      }
      return this;
    }

    protected Builder(Path path) {
      super(path);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected ReadSupport<T> getReadSupport() {
      return new ProtoReadSupport();
    }
  }
}
