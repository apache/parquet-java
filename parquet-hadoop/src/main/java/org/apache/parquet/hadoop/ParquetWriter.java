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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

/**
 * Write records to a Parquet file.
 */
public class ParquetWriter<T> implements Closeable {

  public static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
  public static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024;
  public static final CompressionCodecName DEFAULT_COMPRESSION_CODEC_NAME =
      CompressionCodecName.UNCOMPRESSED;
  public static final boolean DEFAULT_IS_DICTIONARY_ENABLED = true;
  public static final boolean DEFAULT_IS_VALIDATING_ENABLED = false;
  public static final WriterVersion DEFAULT_WRITER_VERSION =
      WriterVersion.PARQUET_1_0;
  public static final long DEFAULT_MAX_ROW_COUNT = (long)Double.POSITIVE_INFINITY;

  // max size (bytes) to write as padding and the min size of a row group
  public static final int MAX_PADDING_SIZE_DEFAULT = 0;

  private final InternalParquetRecordWriter<T> writer;

  /**
   * Create a new ParquetWriter.
   * (with dictionary encoding enabled and validation off)
   *
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize the block size threshold
   * @param pageSize the page size threshold
   * @throws IOException
   * @see #ParquetWriter(Path, WriteSupport, CompressionCodecName, int, int, boolean, boolean)
   */
  @Deprecated
  public ParquetWriter(Path file, WriteSupport<T> writeSupport, CompressionCodecName compressionCodecName, int blockSize, int pageSize) throws IOException {
    this(file, writeSupport, compressionCodecName, blockSize, pageSize,
        DEFAULT_IS_DICTIONARY_ENABLED, DEFAULT_IS_VALIDATING_ENABLED);
  }

  /**
   * Create a new ParquetWriter.
   *
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize the block size threshold
   * @param pageSize the page size threshold (both data and dictionary)
   * @param enableDictionary to turn dictionary encoding on
   * @param validating to turn on validation using the schema
   * @throws IOException
   * @see #ParquetWriter(Path, WriteSupport, CompressionCodecName, int, int, int, boolean, boolean)
   */
  @Deprecated
  public ParquetWriter(
      Path file,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      boolean enableDictionary,
      boolean validating) throws IOException {
    this(file, writeSupport, compressionCodecName, blockSize, pageSize, pageSize, enableDictionary, validating);
  }

  /**
   * Create a new ParquetWriter.
   *
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize the block size threshold
   * @param pageSize the page size threshold
   * @param dictionaryPageSize the page size threshold for the dictionary pages
   * @param enableDictionary to turn dictionary encoding on
   * @param validating to turn on validation using the schema
   * @throws IOException
   * @see #ParquetWriter(Path, WriteSupport, CompressionCodecName, int, int, int, boolean, boolean, WriterVersion)
   */
  @Deprecated
  public ParquetWriter(
      Path file,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating) throws IOException {
    this(file, writeSupport, compressionCodecName, blockSize, pageSize,
        dictionaryPageSize, enableDictionary, validating,
        DEFAULT_WRITER_VERSION);
  }

  /**
   * Create a new ParquetWriter.
   *
   * Directly instantiates a Hadoop {@link org.apache.hadoop.conf.Configuration} which reads
   * configuration from the classpath.
   *
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize the block size threshold
   * @param pageSize the page size threshold
   * @param dictionaryPageSize the page size threshold for the dictionary pages
   * @param enableDictionary to turn dictionary encoding on
   * @param validating to turn on validation using the schema
   * @param writerVersion version of parquetWriter from {@link ParquetProperties.WriterVersion}
   * @throws IOException
   * @see #ParquetWriter(Path, WriteSupport, CompressionCodecName, int, int, int, boolean, boolean, WriterVersion, Configuration)
   */
  @Deprecated
  public ParquetWriter(
      Path file,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion) throws IOException {
    this(file, writeSupport, compressionCodecName, blockSize, pageSize, dictionaryPageSize, enableDictionary, validating, writerVersion, new Configuration());
  }

  /**
   * Create a new ParquetWriter.
   *
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize the block size threshold
   * @param pageSize the page size threshold
   * @param dictionaryPageSize the page size threshold for the dictionary pages
   * @param enableDictionary to turn dictionary encoding on
   * @param validating to turn on validation using the schema
   * @param writerVersion version of parquetWriter from {@link ParquetProperties.WriterVersion}
   * @param conf Hadoop configuration to use while accessing the filesystem
   * @throws IOException
   */
  @Deprecated
  public ParquetWriter(
      Path file,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion,
      Configuration conf) throws IOException {
    this(file, ParquetFileWriter.Mode.CREATE, writeSupport,
        compressionCodecName, blockSize, pageSize, dictionaryPageSize,
        enableDictionary, validating, writerVersion, conf);
  }

  /**
   * Create a new ParquetWriter.
   *
   * @param file the file to create
   * @param mode file creation mode
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize the block size threshold
   * @param pageSize the page size threshold
   * @param dictionaryPageSize the page size threshold for the dictionary pages
   * @param enableDictionary to turn dictionary encoding on
   * @param validating to turn on validation using the schema
   * @param writerVersion version of parquetWriter from {@link ParquetProperties.WriterVersion}
   * @param conf Hadoop configuration to use while accessing the filesystem
   * @throws IOException
   */
  @Deprecated
  public ParquetWriter(
      Path file,
      ParquetFileWriter.Mode mode,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion,
      Configuration conf) throws IOException {
    this(file, mode, writeSupport, compressionCodecName, blockSize, pageSize,
        dictionaryPageSize, enableDictionary, validating, writerVersion, conf,
        MAX_PADDING_SIZE_DEFAULT, DEFAULT_MAX_ROW_COUNT);
  }

  /**
   * Create a new ParquetWriter.  The default block size is 50 MB.The default
   * page size is 1 MB.  Default compression is no compression. Dictionary encoding is disabled.
   *
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @throws IOException
   */
  @Deprecated
  public ParquetWriter(Path file, WriteSupport<T> writeSupport) throws IOException {
    this(file, writeSupport, DEFAULT_COMPRESSION_CODEC_NAME, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
  }

  @Deprecated
  public ParquetWriter(Path file, Configuration conf, WriteSupport<T> writeSupport) throws IOException {
    this(file,
        writeSupport,
        DEFAULT_COMPRESSION_CODEC_NAME,
        DEFAULT_BLOCK_SIZE,
        DEFAULT_PAGE_SIZE,
        DEFAULT_PAGE_SIZE,
        DEFAULT_IS_DICTIONARY_ENABLED,
        DEFAULT_IS_VALIDATING_ENABLED,
        DEFAULT_WRITER_VERSION,
        conf);
  }

  ParquetWriter(
      Path file,
      ParquetFileWriter.Mode mode,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion,
      Configuration conf,
      int maxPaddingSize,
      long maxRowCount) throws IOException {

    WriteSupport.WriteContext writeContext = writeSupport.init(conf);
    MessageType schema = writeContext.getSchema();

    ParquetFileWriter fileWriter = new ParquetFileWriter(
        conf, schema, file, mode, blockSize, maxPaddingSize);
    fileWriter.start();

    CodecFactory codecFactory = new CodecFactory(conf);
    CodecFactory.BytesCompressor compressor =	codecFactory.getCompressor(compressionCodecName, 0);
    this.writer = new InternalParquetRecordWriter<T>(
        fileWriter,
        writeSupport,
        schema,
        writeContext.getExtraMetaData(),
        blockSize,
        pageSize,
        compressor,
        dictionaryPageSize,
        enableDictionary,
        validating,
        writerVersion,
        maxRowCount);
  }

  public void write(T object) throws IOException {
    try {
      writer.write(object);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      writer.close();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * @return the total size of data written to the file and buffered in memory
   */
  public long getDataSize() {
    return writer.getDataSize();
  }

  /**
   * An abstract builder class for ParquetWriter instances.
   *
   * Object models should extend this builder to provide writer configuration
   * options.
   *
   * @param <T> The type of objects written by the constructed ParquetWriter.
   * @param <SELF> The type of this builder that is returned by builder methods
   */
  public abstract static class Builder<T, SELF extends Builder<T, SELF>> {
    private final Path file;
    private Configuration conf = new Configuration();
    private ParquetFileWriter.Mode mode;
    private CompressionCodecName codecName = DEFAULT_COMPRESSION_CODEC_NAME;
    private int rowGroupSize = DEFAULT_BLOCK_SIZE;
    private int pageSize = DEFAULT_PAGE_SIZE;
    private int dictionaryPageSize = DEFAULT_PAGE_SIZE;
    private int maxPaddingSize = MAX_PADDING_SIZE_DEFAULT;
    private long maxRowCount = DEFAULT_MAX_ROW_COUNT;
    private boolean enableDictionary = DEFAULT_IS_DICTIONARY_ENABLED;
    private boolean enableValidation = DEFAULT_IS_VALIDATING_ENABLED;
    private WriterVersion writerVersion = DEFAULT_WRITER_VERSION;

    protected Builder(Path file) {
      this.file = file;
    }

    /**
     * @return this as the correct subclass of ParquetWriter.Builder.
     */
    protected abstract SELF self();

    /**
     * @return an appropriate WriteSupport for the object model.
     */
    protected abstract WriteSupport<T> getWriteSupport(Configuration conf);

    /**
     * Set the {@link Configuration} used by the constructed writer.
     *
     * @param conf a {@code Configuration}
     * @return this builder for method chaining.
     */
    public SELF withConf(Configuration conf) {
      this.conf = conf;
      return self();
    }

    /**
     * Set the {@link ParquetFileWriter.Mode write mode} used when creating the
     * backing file for this writer.
     *
     * @param mode a {@code ParquetFileWriter.Mode}
     * @return this builder for method chaining.
     */
    public SELF withWriteMode(ParquetFileWriter.Mode mode) {
      this.mode = mode;
      return self();
    }

    /**
     * Set the {@link CompressionCodecName compression codec} used by the
     * constructed writer.
     *
     * @param codecName a {@code CompressionCodecName}
     * @return this builder for method chaining.
     */
    public SELF withCompressionCodec(CompressionCodecName codecName) {
      this.codecName = codecName;
      return self();
    }

    /**
     * Set the Parquet format row group size used by the constructed writer.
     *
     * @param rowGroupSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withRowGroupSize(int rowGroupSize) {
      this.rowGroupSize = rowGroupSize;
      return self();
    }

    /**
     * Set the Parquet format page size used by the constructed writer.
     *
     * @param pageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withPageSize(int pageSize) {
      this.pageSize = pageSize;
      return self();
    }

    /**
     * Set the Parquet format dictionary page size used by the constructed
     * writer.
     *
     * @param dictionaryPageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withDictionaryPageSize(int dictionaryPageSize) {
      this.dictionaryPageSize = dictionaryPageSize;
      return self();
    }

    /**
     * Set the maximum amount of padding, in bytes, that will be used to align
     * row groups with blocks in the underlying filesystem. If the underlying
     * filesystem is not a block filesystem like HDFS, this has no effect.
     *
     * @param maxPaddingSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withMaxPaddingSize(int maxPaddingSize) {
      this.maxPaddingSize = maxPaddingSize;
      return self();
    }

    /**
     * Enables dictionary encoding for the constructed writer.
     *
     * @return this builder for method chaining.
     */
    public SELF enableDictionaryEncoding() {
      this.enableDictionary = true;
      return self();
    }

    /**
     * Enable or disable dictionary encoding for the constructed writer.
     *
     * @param enableDictionary whether dictionary encoding should be enabled
     * @return this builder for method chaining.
     */
    public SELF withDictionaryEncoding(boolean enableDictionary) {
      this.enableDictionary = enableDictionary;
      return self();
    }

    /**
     * Enables validation for the constructed writer.
     *
     * @return this builder for method chaining.
     */
    public SELF enableValidation() {
      this.enableValidation = true;
      return self();
    }

    /**
     * Enable or disable validation for the constructed writer.
     *
     * @param enableValidation whether validation should be enabled
     * @return this builder for method chaining.
     */
    public SELF withValidation(boolean enableValidation) {
      this.enableValidation = enableValidation;
      return self();
    }

    /**
     * Set the {@link WriterVersion format version} used by the constructed
     * writer.
     *
     * @param version a {@code WriterVersion}
     * @return this builder for method chaining.
     */
    public SELF withWriterVersion(WriterVersion version) {
      this.writerVersion = version;
      return self();
    }

    /**
    * Set the maximum number of rows per row group
    *
    * @param maxPaddingSize a long (number of rows)
    * @return this builder for method chaining.
     */
    public SELF withMaxRowCount(long maxRowCount) {
      this.maxRowCount = maxRowCount;
      return self();
    }

    /**
     * Build a {@link ParquetWriter} with the accumulated configuration.
     *
     * @return a configured {@code ParquetWriter} instance.
     * @throws IOException
     */
    public ParquetWriter<T> build() throws IOException {
      return new ParquetWriter<T>(file, mode, getWriteSupport(conf), codecName,
          rowGroupSize, pageSize, dictionaryPageSize, enableDictionary,
          enableValidation, writerVersion, conf, maxPaddingSize, maxRowCount);
    }
  }
}
