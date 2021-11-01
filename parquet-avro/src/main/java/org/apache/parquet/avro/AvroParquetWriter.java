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
package org.apache.parquet.avro;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

/**
 * Write Avro records to a Parquet file.
 */
public class AvroParquetWriter<T> extends ParquetWriter<T> {


  /**
   * @param file a file path
   * @param <T> the Java type of records to read from the file
   * @return an Avro reader builder
   * @deprecated will be removed in 2.0.0; use {@link #builder(OutputFile)} instead.
   */
  @Deprecated
  public static <T> Builder<T> builder(Path file) {
    return new Builder<T>(file);
  }

  public static <T> Builder<T> builder(OutputFile file) {
    return new Builder<T>(file);
  }

  /** Create a new {@link AvroParquetWriter}.
   *
   * @param file a file path
   * @param avroSchema a schema for the write
   * @param compressionCodecName compression codec
   * @param blockSize target block size
   * @param pageSize target page size
   * @throws IOException if there is an error while writing
   */
  @Deprecated
  public AvroParquetWriter(Path file, Schema avroSchema,
      CompressionCodecName compressionCodecName, int blockSize,
      int pageSize) throws IOException {
    super(file, AvroParquetWriter.<T>writeSupport(avroSchema, SpecificData.get()),
	      compressionCodecName, blockSize, pageSize);
  }

  /** Create a new {@link AvroParquetWriter}.
   *
   * @param file The file name to write to.
   * @param avroSchema The schema to write with.
   * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
   * @param blockSize the block size threshold.
   * @param pageSize See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
   * @param enableDictionary Whether to use a dictionary to compress columns.
   * @throws IOException if there is an error while writing
   */
  @Deprecated
  public AvroParquetWriter(Path file, Schema avroSchema,
                           CompressionCodecName compressionCodecName, int blockSize,
                           int pageSize, boolean enableDictionary) throws IOException {
    super(file, AvroParquetWriter.<T>writeSupport(avroSchema, SpecificData.get()),
        compressionCodecName, blockSize, pageSize, enableDictionary,
        DEFAULT_IS_VALIDATING_ENABLED);
  }

  /** Create a new {@link AvroParquetWriter}. The default block size is 128 MB. The default
   *  page size is 1 MB. Default compression is no compression. (Inherited from {@link ParquetWriter})
   *
   * @param file The file name to write to.
   * @param avroSchema The schema to write with.
   * @throws IOException if there is an error while writing
   */
  @Deprecated
  public AvroParquetWriter(Path file, Schema avroSchema) throws IOException {
    this(file, avroSchema, CompressionCodecName.UNCOMPRESSED,
        DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
  }

  /** Create a new {@link AvroParquetWriter}.
   *
   * @param file The file name to write to.
   * @param avroSchema The schema to write with.
   * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
   * @param blockSize the block size threshold.
   * @param pageSize See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
   * @param enableDictionary Whether to use a dictionary to compress columns.
   * @param conf The Configuration to use.
   * @throws IOException if there is an error while writing
   */
  @Deprecated
  public AvroParquetWriter(Path file, Schema avroSchema,
                           CompressionCodecName compressionCodecName,
                           int blockSize, int pageSize, boolean enableDictionary,
                           Configuration conf) throws IOException {
    this(file,
        AvroParquetWriter.<T>writeSupport(conf, avroSchema, SpecificData.get()),
        compressionCodecName, blockSize, pageSize,
        enableDictionary, DEFAULT_IS_VALIDATING_ENABLED, DEFAULT_WRITER_VERSION,
        conf);
  }

  /**
   * Create a new {@link AvroParquetWriter}.
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
  AvroParquetWriter(Path file, WriteSupport<T> writeSupport,
                           CompressionCodecName compressionCodecName,
                           int blockSize, int pageSize, boolean enableDictionary,
                           boolean enableValidation, WriterVersion writerVersion,
                           Configuration conf)
      throws IOException {
    super(file, writeSupport, compressionCodecName, blockSize, pageSize,
        pageSize, enableDictionary, enableValidation, writerVersion, conf);
  }

  private static <T> WriteSupport<T> writeSupport(Schema avroSchema,
                                                  GenericData model) {
    return new AvroWriteSupport<T>(
        new AvroSchemaConverter().convert(avroSchema), avroSchema, model);
  }

  private static <T> WriteSupport<T> writeSupport(Configuration conf,
                                                  Schema avroSchema,
                                                  GenericData model) {
    return new AvroWriteSupport<T>(
        new AvroSchemaConverter(conf).convert(avroSchema), avroSchema, model);
  }

  public static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {
    private Schema schema = null;
    private GenericData model = SpecificData.get();

    private Builder(Path file) {
      super(file);
    }

    private Builder(OutputFile file) {
      super(file);
    }

    public Builder<T> withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder<T> withDataModel(GenericData model) {
      this.model = model;
      return this;
    }

    @Override
    protected Builder<T> self() {
      return this;
    }

    @Override
    protected WriteSupport<T> getWriteSupport(Configuration conf) {
      return AvroParquetWriter.writeSupport(conf, schema, model);
    }
  }
}
