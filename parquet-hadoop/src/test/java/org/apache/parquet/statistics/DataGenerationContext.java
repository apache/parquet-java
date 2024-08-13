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

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

public class DataGenerationContext {
  public abstract static class WriteContext {
    protected final File path;
    protected final Path fsPath;
    protected final MessageType schema;
    protected final int blockSize;
    protected final int pageSize;
    protected final boolean enableDictionary;
    protected final boolean enableValidation;
    protected final ParquetProperties.WriterVersion version;
    protected final Set<String> disableColumnStatistics;

    public WriteContext(
        File path,
        MessageType schema,
        int blockSize,
        int pageSize,
        boolean enableDictionary,
        boolean enableValidation,
        ParquetProperties.WriterVersion version)
        throws IOException {
      this(path, schema, blockSize, pageSize, enableDictionary, enableValidation, version, ImmutableSet.of());
    }

    public WriteContext(
        File path,
        MessageType schema,
        int blockSize,
        int pageSize,
        boolean enableDictionary,
        boolean enableValidation,
        ParquetProperties.WriterVersion version,
        Set<String> disableColumnStatistics)
        throws IOException {
      this.path = path;
      this.fsPath = new Path(path.toString());
      this.schema = schema;
      this.blockSize = blockSize;
      this.pageSize = pageSize;
      this.enableDictionary = enableDictionary;
      this.enableValidation = enableValidation;
      this.version = version;
      this.disableColumnStatistics = disableColumnStatistics;
    }

    public abstract void write(ParquetWriter<Group> writer) throws IOException;

    public abstract void test() throws IOException;
  }

  public static void writeAndTest(WriteContext context) throws IOException {
    // Create the configuration, and then apply the schema to our configuration.
    Configuration configuration = new Configuration();

    // Create the writer properties
    final int blockSize = context.blockSize;
    final int pageSize = context.pageSize;
    final int dictionaryPageSize = pageSize;
    final boolean enableDictionary = context.enableDictionary;
    final boolean enableValidation = context.enableValidation;
    ParquetProperties.WriterVersion writerVersion = context.version;
    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(context.fsPath)
        .withType(context.schema)
        .withCompressionCodec(codec)
        .withRowGroupSize(blockSize)
        .withPageSize(pageSize)
        .withDictionaryPageSize(dictionaryPageSize)
        .withDictionaryEncoding(enableDictionary)
        .withValidation(enableValidation)
        .withWriterVersion(writerVersion)
        .withConf(configuration)
        .build()) {
      context.write(writer);
    }

    context.test();

    context.path.delete();
  }
}
