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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import parquet.Log;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.api.WriteSupport.WriteContext;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.util.ContextUtil;

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
 * parquet.block.size=52428800 # in bytes, default = 50 * 1024 * 1024
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

  public static void setWriteSupportClass(Job job,  Class<?> writeSupportClass) {
    ContextUtil.getConfiguration(job).set(WRITE_SUPPORT_CLASS, writeSupportClass.getName());
  }

  public static Class<?> getWriteSupportClass(JobContext jobContext) {
    final String className = ContextUtil.getConfiguration(jobContext).get(WRITE_SUPPORT_CLASS);
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
    ContextUtil.getConfiguration(job).setInt(BLOCK_SIZE, blockSize);
  }

  public static void setPageSize(Job job, int pageSize) {
    ContextUtil.getConfiguration(job).setInt(PAGE_SIZE, pageSize);
  }

  public static void setCompression(Job job, CompressionCodecName compression) {
    ContextUtil.getConfiguration(job).set(COMPRESSION, compression.name());
  }

  public static void setEnableDictionary(Job job, boolean enableDictionary) {
    ContextUtil.getConfiguration(job).setBoolean(ENABLE_DICTIONARY, enableDictionary);
  }

  public static boolean getEnableDictionary(JobContext jobContext) {
    return ContextUtil.getConfiguration(jobContext).getBoolean(ENABLE_DICTIONARY, false);
  }

  public static int getBlockSize(JobContext jobContext) {
    return ContextUtil.getConfiguration(jobContext).getInt(BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE);
  }

  public static int getPageSize(JobContext jobContext) {
    return ContextUtil.getConfiguration(jobContext).getInt(PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
  }

  public static CompressionCodecName getCompression(JobContext jobContext) {
    return CompressionCodecName.fromConf(ContextUtil.getConfiguration(jobContext).get(COMPRESSION, CompressionCodecName.UNCOMPRESSED.name()));
  }

  public static boolean isCompressionSet(JobContext jobContext) {
    return ContextUtil.getConfiguration(jobContext).get(COMPRESSION) != null;
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
    this.writeSupport = writeSupport;
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
    return getRecordWriter(taskAttemptContext, null);
  }

  @SuppressWarnings("unchecked") // writeSupport instantiation
  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext, Path file)
        throws IOException, InterruptedException {
    final Configuration conf = ContextUtil.getConfiguration(taskAttemptContext);
    CodecFactory codecFactory = new CodecFactory(conf);
    int blockSize = getBlockSize(taskAttemptContext);
    if (INFO) LOG.info("Parquet block size to " + blockSize);
    int pageSize = getPageSize(taskAttemptContext);
    if (INFO) LOG.info("Parquet page size to " + pageSize);

    if (writeSupport == null) {
      Class<?> writeSupportClass = getWriteSupportClass(taskAttemptContext);
      try {
        writeSupport = (WriteSupport<T>) writeSupportClass.newInstance();
      } catch (InstantiationException e) {
        throw new BadConfigurationException("could not instantiate " + writeSupportClass.getName(), e);
      } catch (IllegalAccessException e) {
        throw new BadConfigurationException("Illegal access to class " + writeSupportClass.getName(), e);
      }
    }

    String extension = ".parquet";
    CompressionCodecName codec;
    if (isCompressionSet(taskAttemptContext)) { // explicit parquet config
      codec = getCompression(taskAttemptContext);
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
    extension += codec.getExtension();
    if (file == null) {
      file = getDefaultWorkFile(taskAttemptContext, extension);
    }
    boolean enableDictionary = getEnableDictionary(taskAttemptContext);
    WriteContext init = writeSupport.init(conf);
    ParquetFileWriter w = new ParquetFileWriter(conf, init.getSchema(), file);
    w.start();
    return new ParquetRecordWriter<T>(
        w,
        writeSupport,
        init.getSchema(),
        init.getExtraMetaData(),
        blockSize, pageSize,
        codecFactory.getCompressor(codec, pageSize),
        enableDictionary);
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
