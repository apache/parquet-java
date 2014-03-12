/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.column.ParquetProperties;
import parquet.column.ParquetProperties.ParquetPropertiesBuilder;
import parquet.column.ParquetProperties.WriterVersion;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

/**
 * Write records to a Parquet file.
 */
public class ParquetWriter<T> implements Closeable {

  private final InternalParquetRecordWriter<T> writer;
  
  @Deprecated
  public static final int DEFAULT_BLOCK_SIZE = ParquetProperties.DEFAULT_BLOCK_SIZE;
  @Deprecated
  public static final int DEFAULT_PAGE_SIZE = ParquetProperties.DEFAULT_PAGE_SIZE;
  
  public static final CompressionCodecName DEFAULT_COMPRESSION_CODEC_NAME = CompressionCodecName.UNCOMPRESSED;

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
    this(file, 
        writeSupport, 
        compressionCodecName, 
        new Configuration(),
        new ParquetProperties.ParquetPropertiesBuilder()
        .setBlockSize(blockSize)
        .setPageSize(pageSize)
        .build());
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
    this(file, 
        writeSupport, 
        compressionCodecName, 
        new Configuration(),
        new ParquetProperties.ParquetPropertiesBuilder()
        .setBlockSize(blockSize)
        .setPageSize(pageSize)
        .setDictionaryPageSize(dictionaryPageSize)
        .setEnableDictionary(enableDictionary)
        .setValidating(validating)
        .build());
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
    this(file, 
        writeSupport, 
        compressionCodecName, 
        new Configuration(),
        new ParquetProperties.ParquetPropertiesBuilder()
        .setBlockSize(blockSize)
        .setPageSize(pageSize)
        .setDictionaryPageSize(dictionaryPageSize)
        .setEnableDictionary(enableDictionary)
        .setValidating(validating)
        .setWriterVersion(writerVersion)
        .build());
  }

  /**
   * Create a new ParquetWriter.
   * 
   * All the user configurable properties can be specified using parquetProperties.
   * {@link ParquetPropertiesBuilder} can be used to initialize an instance of {@link ParquetProperties}
   * 
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param conf Hadoop configuration to use while accessing the filesystem
   * @param parquetProperties user configurable parquet properties
   * @throws IOException
   */
  public ParquetWriter(Path file,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      Configuration conf,
      ParquetProperties parquetProperties) throws IOException {
    WriteSupport.WriteContext writeContext = writeSupport.init(conf);
    MessageType schema = writeContext.getSchema();

    ParquetFileWriter fileWriter = new ParquetFileWriter(conf, schema, file);
    fileWriter.start();

    CodecFactory codecFactory = new CodecFactory(conf);
    CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(compressionCodecName, 0);
    this.writer = new InternalParquetRecordWriter<T>(
        fileWriter,
        writeSupport,
        schema,
        writeContext.getExtraMetaData(),
        compressor,
        parquetProperties);
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
    this(file, 
        writeSupport, 
        compressionCodecName, 
        conf,
        new ParquetProperties.ParquetPropertiesBuilder()
        .setBlockSize(blockSize)
        .setPageSize(pageSize)
        .setDictionaryPageSize(dictionaryPageSize)
        .setEnableDictionary(enableDictionary)
        .setValidating(validating)
        .setWriterVersion(writerVersion)
        .build());
  }

  /**
   * Create a new ParquetWriter.  The default block size is 50 MB.The default
   * page size is 1 MB.  Default compression is no compression. Dictionary encoding is disabled.
   *
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @throws IOException
   */
  public ParquetWriter(Path file, WriteSupport<T> writeSupport) throws IOException {
    this(file, 
        writeSupport, 
        DEFAULT_COMPRESSION_CODEC_NAME, 
        new Configuration(),
        new ParquetProperties.ParquetPropertiesBuilder()
        .build());
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
