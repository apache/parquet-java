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
package redelm.hadoop;

import static redelm.Log.INFO;

import java.io.IOException;
import java.util.Map;

import redelm.Log;
import redelm.hadoop.metadata.CompressionCodecName;
import redelm.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * OutputFormat to write to a RedElm file
 *
 * it requires a {@link WriteSupport} to convert the actual records to the underlying format
 * it requires the schema of the incoming records
 * it allows storing extra metadata in the footer (for example: for schema compatibility purpose when converting from a different schema language)
 *
 * format config controlled in job conf settings:
 * <pre>
 * redelm.block.size=52428800 # in bytes, default = 50 * 1024 * 1024
 * redelm.page.size=8192 # in bytes, default = 8 * 1024
 * redelm.compression=UNCOMPRESSED # one of: UNCOMPRESSED, SNAPPY, GZIP, LZO. Default: UNCOMPRESSED. Supersedes mapred.output.compress*
 * </pre>
 *
 * If redelm.compression is not set, the following properties are checked (FileOutputFormat behavior)
 * <pre>
 * mapred.output.compress=true
 * mapred.output.compression.codec=org.apache.hadoop.io.compress.SomeCodec
 * </pre>
 *
 * if none of those is set the data is uncompressed.
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class RedelmOutputFormat<T> extends FileOutputFormat<Void, T> {
  private static final Log LOG = Log.getLog(RedelmOutputFormat.class);

  public static final String BLOCK_SIZE = "redelm.block.size";
  public static final String PAGE_SIZE = "redelm.page.size";
  public static final String COMPRESSION = "redelm.compression";

  public static void setBlockSize(Job job, int blockSize) {
    job.getConfiguration().setInt(BLOCK_SIZE, blockSize);
  }

  public static void setPageSize(Job job, int pageSize) {
    job.getConfiguration().setInt(PAGE_SIZE, pageSize);
  }

  public static void setCompression(Job job, CompressionCodecName compression) {
    job.getConfiguration().set(COMPRESSION, compression.name());
  }

  public static int getBlockSize(JobContext jobContext) {
    return jobContext.getConfiguration().getInt(BLOCK_SIZE, 50*1024*1024);
  }

  public static int getPageSize(JobContext jobContext) {
    return jobContext.getConfiguration().getInt(PAGE_SIZE, 8*1024);
  }

  public static CompressionCodecName getCompression(JobContext jobContext) {
    return CompressionCodecName.fromConf(jobContext.getConfiguration().get(COMPRESSION, CompressionCodecName.UNCOMPRESSED.name()));
  }

  public static boolean isCompressionSet(JobContext jobContext) {
    return jobContext.getConfiguration().get(COMPRESSION) != null;
  }

  private final MessageType schema;
  private Class<?> writeSupportClass;

  private final Map<String, String> extraMetaData;
  private RedelmOutputCommitter committer;

  /**
   * constructor used when this OutputFormat in wrapped in another one (In Pig for example)
   * TODO: stand-alone constructor
   * @param writeSupportClass the class used to convert the incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to be stored in the footer of the file
   */
  public <S extends WriteSupport<T>> RedelmOutputFormat(Class<S> writeSupportClass, MessageType schema, Map<String, String> extraMetaData) {
    this.writeSupportClass = writeSupportClass;
    this.schema = schema;
    this.extraMetaData = extraMetaData;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked") // writeSupport instantiation
  @Override
  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    final Configuration conf = taskAttemptContext.getConfiguration();
    CodecFactory codecFactory = new CodecFactory(conf);
    int blockSize = getBlockSize(taskAttemptContext);
    if (INFO) LOG.info("RedElm block size to " + blockSize);
    int pageSize = getPageSize(taskAttemptContext);
    if (INFO) LOG.info("RedElm page size to " + pageSize);

    String extension = ".redelm";
    CompressionCodecName codec;
    if (isCompressionSet(taskAttemptContext)) { // explicit redelm config
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
    final Path file = getDefaultWorkFile(taskAttemptContext, extension);

    final RedelmFileWriter w = new RedelmFileWriter(conf, schema, file);
    w.start();
    try {
      return new RedelmRecordWriter<T>(w, (WriteSupport<T>) writeSupportClass.newInstance(), schema, extraMetaData, blockSize, pageSize, codecFactory.getCompressor(codec, pageSize));
    } catch (InstantiationException e) {
      throw new RuntimeException("could not instantiate " + writeSupportClass.getName(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Illegal access to class " + writeSupportClass.getName(), e);
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    if (committer == null) {
      Path output = getOutputPath(context);
      committer = new RedelmOutputCommitter(output, context);
    }
    return committer;
  }
}
