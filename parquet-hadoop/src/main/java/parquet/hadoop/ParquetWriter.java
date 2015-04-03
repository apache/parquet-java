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
package parquet.hadoop;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.column.ParquetProperties;
import parquet.column.ParquetProperties.WriterVersion;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
import static parquet.Preconditions.checkNotNull;

/**
 * Write records to a Parquet file.
 */
public class ParquetWriter<T> implements Closeable {

  public static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
  public static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024;
  public static final int DEFAULT_DICTIONARY_PAGE_SIZE = 1 * 1024 * 1024;
  public static final CompressionCodecName DEFAULT_COMPRESSION_CODEC_NAME =
      CompressionCodecName.UNCOMPRESSED;
  public static final boolean DEFAULT_IS_DICTIONARY_ENABLED = true;
  public static final boolean DEFAULT_IS_VALIDATING_ENABLED = false;
  public static final WriterVersion DEFAULT_WRITER_VERSION =
      WriterVersion.PARQUET_1_0;

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
   * @deprecated use {@link #builder(WriteSupport<T>, Path)}
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
   * @deprecated use {@link #builder(WriteSupport<T>, Path)}
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
   * @deprecated use {@link #builder(WriteSupport<T>, Path)}
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
   * @deprecated use {@link #builder(WriteSupport<T>, Path)}
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
   * @deprecated use {@link #builder(WriteSupport<T>, Path)}
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
   * @deprecated use {@link #builder(WriteSupport<T>, Path)}
   */
  @Deprecated
  // FIXME: make this constructor protected upon removing the deprecated stuff
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

    WriteSupport.WriteContext writeContext = writeSupport.init(conf);
    MessageType schema = writeContext.getSchema();

    ParquetFileWriter fileWriter = new ParquetFileWriter(conf, schema, file,
        mode);
    fileWriter.start();

    CodecFactory codecFactory = new CodecFactory(conf);
    CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(compressionCodecName, 0);
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
        writerVersion);
  }

  /**
   * Create a new ParquetWriter.  The default block size is 50 MB.The default
   * page size is 1 MB.  Default compression is no compression. Dictionary encoding is disabled.
   *
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @throws IOException
   * @deprecated use {@link #builder(WriteSupport<T>, Path)}
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
  
  /**
   * Builder for the ParquetWriter class
   * It is meant to be inherited by the builders of ParquetWriter's subclasses
   */
  protected static class Builder<T> {
    // Maybe it will be wiser to make these private
    // and provide access to the subclasses through getters.
    protected Path file;
    protected ParquetFileWriter.Mode mode;
    protected WriteSupport<T> writeSupport;
    protected CompressionCodecName compressionCodecName;
    protected int blockSize;
    protected int pageSize;
    protected int dictionaryPageSize;
    protected boolean enableDictionary;
    protected boolean enableValidation;
    protected WriterVersion writerVersion;
    protected Configuration conf;

    /**
     * @param writeSupport the implementation to write a record to a RecordConsumer
     * @param file the file to create
     */
    public Builder(WriteSupport<T> writeSupport, Path file) {
      writeSupport = checkNotNull(writeSupport, "writeSupport");
      file = checkNotNull(file, "file");
      mode = ParquetFileWriter.Mode.CREATE;
      compressionCodecName = DEFAULT_COMPRESSION_CODEC_NAME;
      blockSize = DEFAULT_BLOCK_SIZE;
      pageSize = DEFAULT_PAGE_SIZE;
      dictionaryPageSize = DEFAULT_DICTIONARY_PAGE_SIZE;
      enableDictionary = DEFAULT_IS_DICTIONARY_ENABLED;
      enableValidation = DEFAULT_IS_VALIDATING_ENABLED;
      writerVersion = DEFAULT_WRITER_VERSION;
      conf = new Configuration();
    }

    /**
     * @param mode file creation mode
     */
    public Builder<T> setFileWriteMode(ParquetFileWriter.Mode mode) {
      this.mode = checkNotNull(mode, "mode");
      return this;
    }

    /**
     * @param compressionCodecName the compression codec to use
     */
    public Builder<T> setCompressionCodec(CompressionCodecName compressionCodecName) {
      this.compressionCodecName = checkNotNull(compressionCodecName, "compressionCodecName");
      return this;
    }

    /**
     * @param blockSize the block size threshold
     */
    public Builder<T> setBlockSize(int blockSize) {
      this.blockSize = blockSize;
      return this;
    }

    /**
     * @param pageSize the page size threshold
     * @return
     */
    public Builder<T> setPageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    /**
     * @param dictionaryPageSize the page size threshold for the dictionary pages
     */
    public Builder<T> setDictionaryPageSize(int dictionaryPageSize) {
      this.dictionaryPageSize = dictionaryPageSize;
      this.enableDictionary = true;
      return this;
    }

    /**
     * @param enableDictionary to turn dictionary encoding on
     */
    public Builder<T> enableDictionary(boolean enableDictionary) {
      this.enableDictionary = enableDictionary;
      return this;
    }

    /**
     * @param enableValidation to turn on validation using the schema
     */
    public Builder<T> enableValidation(boolean enableValidation) {
      this.enableValidation = enableValidation;
      return this;
    }

    /**
     * @param writerVersion version of parquetWriter from {@link ParquetProperties.WriterVersion}
     */
    public Builder<T> setWriterVersion(WriterVersion writerVersion) {
      this.writerVersion = checkNotNull(writerVersion, "writerVersion");
      return this;
    }

    /**
     * @param conf Hadoop configuration to use while accessing the filesystem
     */
    public Builder<T> setConfiguration(Configuration conf) {
      this.conf = checkNotNull(conf, "conf");
      return this;
    }
    
    /**
     * @return a new instance of {@link ParquetWriter<T>}
     * @throws IOException
     */
    public ParquetWriter<T> build() throws IOException {
      return new ParquetWriter<T>(file, mode, writeSupport, compressionCodecName, blockSize,
          pageSize, dictionaryPageSize, enableDictionary, enableValidation, writerVersion, conf);
    }
  }
  
  /**
   * Convenience method for getting a new builder
   */
  public static <T> Builder<T> builder(WriteSupport<T> writeSupport, Path file) {
    return new Builder<T>(writeSupport, file);
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
}
