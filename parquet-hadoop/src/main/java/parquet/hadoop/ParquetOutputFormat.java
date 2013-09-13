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

import static parquet.Log.INFO;
import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
import static parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static parquet.hadoop.metadata.CompressionCodecName.fromConf;
import static parquet.hadoop.util.ContextUtil.getConfiguration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import parquet.Log;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.api.WriteSupport.WriteContext;
import parquet.hadoop.metadata.CompressionCodecName;

/**
 * OutputFormat to write to a Parquet file
 *
 * It requires a {@link WriteSupport} to convert the actual records to the underlying format.
 * It requires the schema of the incoming records. (provided by the write support)
 * It allows storing extra metadata in the footer (for example: for schema compatibility purpose when converting from a different schema language).
 *
 * The format configuration settings in the job configuration:
 * <pre>
 * # The block size is the size of a row group being buffered in memory
 * # this limits the memory usage when writing
 * # Larger values will improve the IO when reading but consume more memory when writing
 * parquet.block.size=134217728 # in bytes, default = 128 * 1024 * 1024
 *
 * # The page size is for compression. When reading, each page can be decompressed independently.
 * # A block is composed of pages. The page is the smallest unit that must be read fully to access a single record.
 * # If this value is too small, the compression will deteriorate
 * parquet.page.size=1048576 # in bytes, default = 1 * 1024 * 1024
 *
 * # The compression algorithm used to compress pages
 * parquet.compression=UNCOMPRESSED # one of: UNCOMPRESSED, SNAPPY, GZIP, LZO. Default: UNCOMPRESSED. Supersedes mapred.output.compress*
 *
 * # The write support class to convert the records written to the OutputFormat into the events accepted by the record consumer
 * # Usually provided by a specific ParquetOutputFormat subclass
 * parquet.write.support.class= # fully qualified name
 *
 * # To enable dictionary encoding
 * parquet.enable.dictionary=false # true to enable dictionary encoding
 * </pre>
 *
 * If parquet.compression is not set, the following properties are checked (FileOutputFormat behavior).
 * Note that we explicitely disallow custom Codecs
 * <pre>
 * mapred.output.compress=true
 * mapred.output.compression.codec=org.apache.hadoop.io.compress.SomeCodec # the codec must be one of Snappy, GZip or LZO
 * </pre>
 *
 * if none of those is set the data is uncompressed.
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class ParquetOutputFormat<T> extends FileOutputFormat<Void, T> {
  private static final Log LOG = Log.getLog(ParquetOutputFormat.class);

  public static final String BLOCK_SIZE          = "parquet.block.size";
  public static final String PAGE_SIZE           = "parquet.page.size";
  public static final String COMPRESSION         = "parquet.compression";
  public static final String WRITE_SUPPORT_CLASS = "parquet.write.support.class";
  public static final String ENABLE_DICTIONARY   = "parquet.enable.dictionary";
  public static final String VALIDATION          = "parquet.validation";

  public static void setWriteSupportClass(Job job,  Class<?> writeSupportClass) {
    getConfiguration(job).set(WRITE_SUPPORT_CLASS, writeSupportClass.getName());
  }

  public static Class<?> getWriteSupportClass(Configuration configuration) {
    final String className = configuration.get(WRITE_SUPPORT_CLASS);
    if (className == null) {
      return null;
    }
    try {
      final Class<?> writeSupportClass = Class.forName(className);
      if (!WriteSupport.class.isAssignableFrom(writeSupportClass)) {
        throw new BadConfigurationException("class " + className + " set in job conf at " + WRITE_SUPPORT_CLASS + " is not a subclass of WriteSupport");
      }
      return writeSupportClass;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("could not instanciate class " + className + " set in job conf at " + WRITE_SUPPORT_CLASS , e);
    }
  }

  public static void setBlockSize(Job job, int blockSize) {
    getConfiguration(job).setInt(BLOCK_SIZE, blockSize);
  }

  public static void setPageSize(Job job, int pageSize) {
    getConfiguration(job).setInt(PAGE_SIZE, pageSize);
  }

  public static void setCompression(Job job, CompressionCodecName compression) {
    getConfiguration(job).set(COMPRESSION, compression.name());
  }

  public static void setEnableDictionary(Job job, boolean enableDictionary) {
    getConfiguration(job).setBoolean(ENABLE_DICTIONARY, enableDictionary);
  }

  public static boolean getEnableDictionary(Configuration configuration) {
    return configuration.getBoolean(ENABLE_DICTIONARY, false);
  }

  public static int getBlockSize(Configuration configuration) {
    return configuration.getInt(BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
  }

  public static int getPageSize(Configuration configuration) {
    return configuration.getInt(PAGE_SIZE, DEFAULT_PAGE_SIZE);
  }

  public static CompressionCodecName getCompression(Configuration configuration) {
    return fromConf(configuration.get(COMPRESSION, UNCOMPRESSED.name()));
  }

  public static boolean isCompressionSet(Configuration configuration) {
    return configuration.get(COMPRESSION) != null;
  }

  public static void setValidation(Configuration configuration, boolean validating) {
    configuration.setBoolean(VALIDATION, validating);
  }

  public static boolean getValidation(Configuration configuration) {
    return configuration.getBoolean(VALIDATION, false);
  }

  private static CompressionCodecName getCodec(TaskAttemptContext taskAttemptContext) {
    Configuration conf = getConfiguration(taskAttemptContext);
    CompressionCodecName codec;

    if (isCompressionSet(conf)) { // explicit parquet config
      codec = getCompression(conf);
    } else if (getCompressOutput(taskAttemptContext)) { // from hadoop config
      // find the right codec
      Class<?> codecClass = getOutputCompressorClass(taskAttemptContext, DefaultCodec.class);
      if (INFO) LOG.info("Compression set through hadoop codec: " + codecClass.getName());
      codec = CompressionCodecName.fromCompressionCodec(codecClass);
    } else {
      if (INFO) LOG.info("Compression set to false");
      codec = CompressionCodecName.UNCOMPRESSED;
    }

    if (INFO) LOG.info("Compression: " + codec.name());
    return codec;
  }


  private WriteSupport<T> writeSupport;
  private ParquetOutputCommitter committer;

  /**
   * constructor used when this OutputFormat in wrapped in another one (In Pig for example)
   * @param writeSupportClass the class used to convert the incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to be stored in the footer of the file
   */
  public <S extends WriteSupport<T>> ParquetOutputFormat(S writeSupport) {
    this.writeSupport= writeSupport;
  }

  /**
   * used when directly using the output format and configuring the write support implementation
   * using parquet.write.support.class
   */
  public <S extends WriteSupport<T>> ParquetOutputFormat() {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {

    final Configuration conf = getConfiguration(taskAttemptContext);

    CompressionCodecName codec = getCodec(taskAttemptContext);
    String extension = codec.getExtension() + ".parquet";
    Path file = getDefaultWorkFile(taskAttemptContext, extension);
    return getRecordWriter(conf, file, codec);
  }

  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext, Path file)
      throws IOException, InterruptedException {
    return getRecordWriter(getConfiguration(taskAttemptContext), file, getCodec(taskAttemptContext));
  }

  @SuppressWarnings("unchecked") // writeSupport instantiation
  public RecordWriter<Void, T> getRecordWriter(Configuration conf, Path file, CompressionCodecName codec)
        throws IOException, InterruptedException {
    final WriteSupport<T> writeSupport = getWriteSupport(conf);

    CodecFactory codecFactory = new CodecFactory(conf);
    int blockSize = getBlockSize(conf);
    if (INFO) LOG.info("Parquet block size to " + blockSize);
    int pageSize = getPageSize(conf);
    if (INFO) LOG.info("Parquet page size to " + pageSize);

    boolean enableDictionary = getEnableDictionary(conf);
    WriteContext init = writeSupport.init(conf);
    ParquetFileWriter w = new ParquetFileWriter(conf, init.getSchema(), file);
    w.start();
    boolean validating = getValidation(conf);
    if (INFO) LOG.info("Validation is " + (validating ? "on" : "off"));

    return new ParquetRecordWriter<T>(
        w,
        writeSupport,
        init.getSchema(),
        init.getExtraMetaData(),
        blockSize, pageSize,
        codecFactory.getCompressor(codec, pageSize),
        enableDictionary,
        validating);
  }

  /**
   * @param configuration to find the configuration for the write support class
   * @return the configured write support
   */
  @SuppressWarnings("unchecked")
  public WriteSupport<T> getWriteSupport(Configuration configuration){
    if (writeSupport != null) return writeSupport;
    try {
      Class<?> writeSupportClass = getWriteSupportClass(configuration);
      return (WriteSupport<T>)writeSupportClass.newInstance();
    } catch (InstantiationException e) {
      throw new BadConfigurationException("could not instantiate write support class", e);
    } catch (IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate write support class", e);
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    if (committer == null) {
      Path output = getOutputPath(context);
      committer = new ParquetOutputCommitter(output, context);
    }
    return committer;
  }
}
