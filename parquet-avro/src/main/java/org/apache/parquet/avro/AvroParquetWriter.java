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

/**
 * Write Avro records to a Parquet file.
 */
public class AvroParquetWriter<T> extends ParquetWriter<T> {

  public static <T> Builder<T> builder(Path file) {
    return new Builder<T>(file);
  }

  /** Create a new {@link AvroParquetWriter}.
   *
   * @param file
   * @param avroSchema
   * @param compressionCodecName
   * @param blockSize
   * @param pageSize
   * @throws IOException
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
   * @throws IOException
   */
  @Deprecated
  public AvroParquetWriter(Path file, Schema avroSchema,
                           CompressionCodecName compressionCodecName, int blockSize,
                           int pageSize, boolean enableDictionary) throws IOException {
    super(file, AvroParquetWriter.<T>writeSupport(avroSchema, SpecificData.get()),
        compressionCodecName, blockSize, pageSize, enableDictionary,
        DEFAULT_IS_VALIDATING_ENABLED);
  }

  /** Create a new {@link AvroParquetWriter}. The default block size is 50 MB.The default
   *  page size is 1 MB.  Default compression is no compression. (Inherited from {@link ParquetWriter})
   *
   * @param file The file name to write to.
   * @param avroSchema The schema to write with.
   * @throws IOException
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
   * @throws IOException
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

  public static class Builder<T> {
    private final Path file;
    private Configuration conf = new Configuration();
    private CompressionCodecName codecName = DEFAULT_COMPRESSION_CODEC_NAME;
    private int blockSize = DEFAULT_BLOCK_SIZE;
    private int pageSize = DEFAULT_PAGE_SIZE;
    private boolean enableDictionary = DEFAULT_IS_DICTIONARY_ENABLED;
    private boolean enableValidation = DEFAULT_IS_VALIDATING_ENABLED;
    private WriterVersion writerVersion = DEFAULT_WRITER_VERSION;

    // avro-specific
    private Schema schema = null;
    private GenericData model = SpecificData.get();

    private Builder(Path file) {
      this.file = file;
    }

    public Builder<T> withConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder<T> withCompressionCodec(CompressionCodecName codecName) {
      this.codecName = codecName;
      return this;
    }

    public Builder<T> withBlockSize(int blockSize) {
      this.blockSize = blockSize;
      return this;
    }

    public Builder<T> withPageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public Builder<T> enableDictionaryEncoding() {
      this.enableDictionary = true;
      return this;
    }

    public Builder<T> withDictionaryEncoding(boolean enableDictionary) {
      this.enableDictionary = enableDictionary;
      return this;
    }

    public Builder<T> enableValidation() {
      this.enableValidation = true;
      return this;
    }

    public Builder<T> withValidation(boolean enableValidation) {
      this.enableValidation = enableValidation;
      return this;
    }

    public Builder<T> withWriterVersion(WriterVersion version) {
      this.writerVersion = version;
      return this;
    }

    public Builder<T> withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder<T> withDataModel(GenericData model) {
      this.model = model;
      return this;
    }

    private WriteSupport<T> getWriteSupport() {
      return AvroParquetWriter.<T>writeSupport(conf, schema, model);
    }

    public ParquetWriter<T> build() throws IOException {
      return new AvroParquetWriter<T>(file, getWriteSupport(), codecName,
          blockSize, pageSize, enableDictionary, enableValidation,
          writerVersion, conf);
    }
  }
}
