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
package org.apache.parquet.statistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;

public class DataGenerationContext {
  public static abstract class WriteContext {
    protected final File path;
    protected final Path fsPath;
    protected final MessageType schema;
    protected final int blockSize;
    protected final int pageSize;
    protected final boolean enableDictionary;
    protected final boolean enableValidation;
    protected final ParquetProperties.WriterVersion version;

    public WriteContext(File path, MessageType schema, int blockSize, int pageSize, boolean enableDictionary, boolean enableValidation, ParquetProperties.WriterVersion version) throws IOException {
      this.path = path;
      this.fsPath = new Path(path.toString());
      this.schema = schema;
      this.blockSize = blockSize;
      this.pageSize = pageSize;
      this.enableDictionary = enableDictionary;
      this.enableValidation = enableValidation;
      this.version = version;
    }

    public abstract void write(ParquetWriter<Group> writer) throws IOException;

    public abstract void test() throws IOException;
  }

  public static void writeAndTest(WriteContext context) throws IOException {
    // Create the configuration, and then apply the schema to our configuration.
    Configuration configuration = new Configuration();
    GroupWriteSupport.setSchema(context.schema, configuration);
    GroupWriteSupport groupWriteSupport = new GroupWriteSupport();

    // Create the writer properties
    final int blockSize = context.blockSize;
    final int pageSize = context.pageSize;
    final int dictionaryPageSize = pageSize;
    final boolean enableDictionary = context.enableDictionary;
    final boolean enableValidation = context.enableValidation;
    ParquetProperties.WriterVersion writerVersion = context.version;
    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;

    try (ParquetWriter<Group> writer = new ParquetWriter<Group>(context.fsPath,
      groupWriteSupport, codec, blockSize, pageSize, dictionaryPageSize,
      enableDictionary, enableValidation, writerVersion, configuration)) {
      context.write(writer);
    }

    context.test();

    context.path.delete();
  }
}
