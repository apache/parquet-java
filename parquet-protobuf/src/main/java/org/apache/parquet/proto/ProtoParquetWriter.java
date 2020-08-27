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
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.MessageOrBuilder;

/**
 * Write Protobuf records to a Parquet file.
 */
public class ProtoParquetWriter<T extends MessageOrBuilder> extends ParquetWriter<T> {

  /**
   * Create a new {@link ProtoParquetWriter}.
   *
   * @param file                 The file name to write to.
   * @param protoMessage         Protobuf message class
   * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
   * @param blockSize            HDFS block size
   * @param pageSize             See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
   * @throws IOException if there is an error while writing
   */
  public ProtoParquetWriter(Path file, Class<? extends Message> protoMessage,
                            CompressionCodecName compressionCodecName, int blockSize,
                            int pageSize) throws IOException {
    super(file, new ProtoWriteSupport(protoMessage),
            compressionCodecName, blockSize, pageSize);
  }

  /**
   * Create a new {@link ProtoParquetWriter}.
   *
   * @param file                 The file name to write to.
   * @param protoMessage         Protobuf message class
   * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
   * @param blockSize            HDFS block size
   * @param pageSize             See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
   * @param enableDictionary     Whether to use a dictionary to compress columns.
   * @param validating           to turn on validation using the schema
   * @throws IOException if there is an error while writing
   */
  public ProtoParquetWriter(Path file, Class<? extends Message> protoMessage,
                            CompressionCodecName compressionCodecName, int blockSize,
                            int pageSize, boolean enableDictionary, boolean validating) throws IOException {
    super(file, new ProtoWriteSupport(protoMessage),
            compressionCodecName, blockSize, pageSize, enableDictionary, validating);
  }

  /**
   * Create a new {@link ProtoParquetWriter}. The default block size is 50 MB.The default
   * page size is 1 MB.  Default compression is no compression. (Inherited from {@link ParquetWriter})
   *
   * @param file The file name to write to.
   * @param protoMessage         Protobuf message class
   * @throws IOException if there is an error while writing
   */
  public ProtoParquetWriter(Path file, Class<? extends Message> protoMessage) throws IOException {
    this(file, protoMessage, CompressionCodecName.UNCOMPRESSED,
            DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
  }

  public static <T extends MessageOrBuilder> Builder<T> builder(Path file) {
    return new Builder<T>(file);
  }

  public static <T extends MessageOrBuilder> Builder<T> builder(Path file, T message) {
    return new Builder<T>(file).withMessage(message);
  }

  public static <T extends MessageOrBuilder> Builder<T> builder(OutputFile file) {
    return new Builder<T>(file);
  }

  private static <T extends MessageOrBuilder> WriteSupport<T> writeSupport(
      Class<? extends Message> clazz, Optional<String> schemaRegistry) {
    return new ProtoWriteSupport<T>(clazz, schemaRegistry);
  }

  private static <T extends MessageOrBuilder> WriteSupport<T> writeSupport(
      MessageOrBuilder message, Optional<String> schemaRegistry) {
    return new ProtoWriteSupport<T>(message, schemaRegistry);
  }

  public static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {

    private Class<? extends Message> clazz = null;
    private MessageOrBuilder message = null;
    private Optional<String> schemaRegistry = Optional.empty();

    private Builder(Path file) {
      super(file);
    }

    private Builder(OutputFile file) {
      super(file);
    }

    protected Builder<T> self() {
      return this;
    }

    @Deprecated
    public Builder<T> withMessage(Class<? extends Message> clazz) {
      this.clazz = clazz;
      return this;
    }

    public Builder<T> withMessage(MessageOrBuilder message) {
      this.message = message;
      return this;
    }

    public Builder<T> withSchemaRegistry(String schemaRegistry) {
      this.schemaRegistry = Optional.ofNullable(schemaRegistry);
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected WriteSupport<T> getWriteSupport(Configuration conf) {
      if (message != null) {
        return (WriteSupport<T>) ProtoParquetWriter.writeSupport(message, this.schemaRegistry);
      }
      return (WriteSupport<T>) ProtoParquetWriter.writeSupport(clazz, this.schemaRegistry);
    }
  }
}
