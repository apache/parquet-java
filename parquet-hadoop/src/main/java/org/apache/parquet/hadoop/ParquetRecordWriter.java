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
package org.apache.parquet.hadoop;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;

import static org.apache.parquet.Preconditions.checkNotNull;

/**
 * Writes records to a Parquet file
 *
 * @see ParquetOutputFormat
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class ParquetRecordWriter<T> extends RecordWriter<Void, T> {

  private InternalParquetRecordWriter<T> internalWriter;
  private MemoryManager memoryManager;

  /**
   *
   * @param w the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param blockSize the size of a block in the file (this will be approximate)
   * @param compressor the compressor used to compress the pages
   * @param dictionaryPageSize the threshold for dictionary size
   * @param enableDictionary to enable the dictionary
   * @param validating if schema validation should be turned on
   */
  @Deprecated
  public ParquetRecordWriter(
      ParquetFileWriter w,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetaData,
      int blockSize, int pageSize,
      BytesCompressor compressor,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion) {
    ParquetProperties props = ParquetProperties.builder()
        .withPageSize(pageSize)
        .withDictionaryPageSize(dictionaryPageSize)
        .withDictionaryEncoding(enableDictionary)
        .withWriterVersion(writerVersion)
        .build();
    internalWriter = new InternalParquetRecordWriter<T>(w, writeSupport, schema,
        extraMetaData, blockSize, compressor, validating, props);
    this.memoryManager = null;
  }

  /**
   *
   * @param w the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param blockSize the size of a block in the file (this will be approximate)
   * @param compressor the compressor used to compress the pages
   * @param dictionaryPageSize the threshold for dictionary size
   * @param enableDictionary to enable the dictionary
   * @param validating if schema validation should be turned on
   */
  @Deprecated
  public ParquetRecordWriter(
      ParquetFileWriter w,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetaData,
      long blockSize, int pageSize,
      BytesCompressor compressor,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion,
      MemoryManager memoryManager) {
    this(w, writeSupport, schema, extraMetaData, blockSize, compressor,
        validating, ParquetProperties.builder()
            .withPageSize(pageSize)
            .withDictionaryPageSize(dictionaryPageSize)
            .withDictionaryEncoding(enableDictionary)
            .withWriterVersion(writerVersion)
            .build(),
        memoryManager);
  }

  /**
   *
   * @param w the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param blockSize the size of a block in the file (this will be approximate)
   * @param compressor the compressor used to compress the pages
   * @param validating if schema validation should be turned on
   * @param props parquet encoding properties
   */
  ParquetRecordWriter(
      ParquetFileWriter w,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetaData,
      long blockSize,
      BytesCompressor compressor,
      boolean validating,
      ParquetProperties props,
      MemoryManager memoryManager) {
    internalWriter = new InternalParquetRecordWriter<T>(w, writeSupport, schema,
        extraMetaData, blockSize, compressor, validating, props);
    this.memoryManager = checkNotNull(memoryManager, "memoryManager");
    memoryManager.addWriter(internalWriter, blockSize);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    internalWriter.close();
    if (memoryManager != null) {
      memoryManager.removeWriter(internalWriter);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(Void key, T value) throws IOException, InterruptedException {
    internalWriter.write(value);
  }

}
