/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

/**
 * An example file writer class.
 * THIS IS AN EXAMPLE ONLY AND NOT INTENDED FOR USE.
 */
public class ExampleParquetWriter extends ParquetWriter<Group> {

  /**
   * Creates a Builder for configuring ParquetWriter with the example object
   * model. THIS IS AN EXAMPLE ONLY AND NOT INTENDED FOR USE.
   *
   * @param file the output file to create
   * @return a {@link Builder} to create a {@link ParquetWriter}
   */
  public static Builder builder(Path file) {
    return new Builder(file);
  }

  /**
   * Creates a Builder for configuring ParquetWriter with the example object
   * model. THIS IS AN EXAMPLE ONLY AND NOT INTENDED FOR USE.
   *
   * @param file the output file to create
   * @return a {@link Builder} to create a {@link ParquetWriter}
   */
  public static Builder builder(OutputFile file) {
    return new Builder(file);
  }

  /**
   * Create a new {@link ExampleParquetWriter}.
   *
   * @param file The file name to write to.
   * @param writeSupport The schema to write with.
   * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
   * @param blockSize the block size threshold.
   * @param pageSize See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
   * @param enableDictionary Whether to use a dictionary to compress columns.
   * @param conf The Configuration to use.
   * @throws IOException
   */
  ExampleParquetWriter(
      Path file,
      WriteSupport<Group> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      boolean enableDictionary,
      boolean enableValidation,
      ParquetProperties.WriterVersion writerVersion,
      Configuration conf)
      throws IOException {
    super(
        file,
        writeSupport,
        compressionCodecName,
        blockSize,
        pageSize,
        pageSize,
        enableDictionary,
        enableValidation,
        writerVersion,
        conf);
  }

  public static class Builder extends ParquetWriter.Builder<Group, Builder> {
    private MessageType type = null;
    private Map<String, String> extraMetaData = new HashMap<String, String>();

    private Builder(Path file) {
      super(file);
    }

    private Builder(OutputFile file) {
      super(file);
    }

    public Builder withType(MessageType type) {
      this.type = type;
      return this;
    }

    public Builder withExtraMetaData(Map<String, String> extraMetaData) {
      this.extraMetaData = extraMetaData;
      return this;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected WriteSupport<Group> getWriteSupport(Configuration conf) {
      return getWriteSupport((ParquetConfiguration) null);
    }

    @Override
    protected WriteSupport<Group> getWriteSupport(ParquetConfiguration conf) {
      return new GroupWriteSupport(type, extraMetaData);
    }
  }
}
