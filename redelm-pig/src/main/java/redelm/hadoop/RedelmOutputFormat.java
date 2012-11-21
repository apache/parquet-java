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
import java.util.List;

import redelm.Log;
import redelm.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * OutputFormat to write to a RedElm file
 *
 * it requires a {@link WriteSupport} to convert the actual records to the underlying format
 * it requires the schema of the incoming records
 * it allows storing extra metadata in the footer (for example: for schema compatibility purpose when converting from a different schema language)
 *
 * data is compressed according to the job conf (per block per column):
 * <pre>
 * mapred.output.compress=true
 * mapred.output.compression.codec=org.apache.hadoop.io.compress.SomeCodec
 * </pre>
 *
 * block size is controlled in job conf settings:
 * <pre>
 * redelm.block.size=52428800 # in bytes, default = 50 * 1024 * 1024
 *</pre>
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class RedelmOutputFormat<T> extends FileOutputFormat<Void, T> {
  private static final Log LOG = Log.getLog(RedelmOutputFormat.class);

  public static final String BLOCK_SIZE = "redelm.block.size";

  public static void setBlockSize(Job job, int blockSize) {
    job.getConfiguration().setInt(BLOCK_SIZE, blockSize);
  }

  public static int getBlockSize(JobContext jobContext) {
    return jobContext.getConfiguration().getInt(BLOCK_SIZE, 50*1024*1024);
  }

  private final MessageType schema;
  private Class<?> writeSupportClass;

  private final List<MetaDataBlock> extraMetaData;

  /**
   * constructor used when this OutputFormat in wrapped in another one (In Pig for example)
   * TODO: standalone constructor
   * @param writeSupportClass the class used to convert the incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to be stored in the footer of the file
   */
  public <S extends WriteSupport<T>> RedelmOutputFormat(Class<S> writeSupportClass, MessageType schema, List<MetaDataBlock> extraMetaData) {
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
    final Path file = getDefaultWorkFile(taskAttemptContext, "");
    final Configuration conf = taskAttemptContext.getConfiguration();
    final FileSystem fs = file.getFileSystem(conf);

    int blockSize = getBlockSize(taskAttemptContext);

    CompressionCodec codec = null;
    if (getCompressOutput(taskAttemptContext)) {
      // find the right codec
      Class<?> codecClass = getOutputCompressorClass(taskAttemptContext, DefaultCodec.class);
      if (INFO) LOG.info("Compression codec: " + codecClass.getName());
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
    }

    final RedelmFileWriter w = new RedelmFileWriter(schema, fs.create(file, false), codec);
    w.start();
    try {
      return new RedelmRecordWriter<T>(w, (WriteSupport<T>) writeSupportClass.newInstance(), schema, extraMetaData, blockSize);
    } catch (InstantiationException e) {
      throw new RuntimeException("could not instantiate " + writeSupportClass.getName(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Illegal access to class " + writeSupportClass.getName(), e);
    }
  }

}
