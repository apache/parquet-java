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
package parquet.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.ParquetRecordWriter;
import parquet.hadoop.codec.CodecConfig;
import parquet.hadoop.metadata.CompressionCodecName;

public class DeprecatedParquetOutputFormat<V> extends org.apache.hadoop.mapred.FileOutputFormat<Void, V> {

  public static void setWriteSupportClass(Configuration configuration,  Class<?> writeSupportClass) {
    configuration.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, writeSupportClass.getName());
  }

  public static void setBlockSize(Configuration configuration, int blockSize) {
    configuration.setInt(ParquetOutputFormat.BLOCK_SIZE, blockSize);
  }

  public static void setPageSize(Configuration configuration, int pageSize) {
    configuration.setInt(ParquetOutputFormat.PAGE_SIZE, pageSize);
  }

  public static void setCompression(Configuration configuration, CompressionCodecName compression) {
    configuration.set(ParquetOutputFormat.COMPRESSION, compression.name());
  }

  public static void setEnableDictionary(Configuration configuration, boolean enableDictionary) {
    configuration.setBoolean(ParquetOutputFormat.ENABLE_DICTIONARY, enableDictionary);
  }

  private CompressionCodecName getCodec(final JobConf conf) {
    return CodecConfig.from(conf).getCodec();
  }

  private static Path getDefaultWorkFile(JobConf conf, String name, String extension) {
    String file = getUniqueName(conf, name) + extension;
    return new Path(getWorkOutputPath(conf), file);
  }

  protected ParquetOutputFormat<V> realOutputFormat = new ParquetOutputFormat<V>();

  @Override
  public RecordWriter<Void, V> getRecordWriter(FileSystem fs,
      JobConf conf, String name, Progressable progress) throws IOException {
    return new RecordWriterWrapper(realOutputFormat, fs, conf, name, progress);
  }

  private class RecordWriterWrapper implements RecordWriter<Void, V> {

    private ParquetRecordWriter<V> realWriter;

    public RecordWriterWrapper(ParquetOutputFormat<V> realOutputFormat,
        FileSystem fs, JobConf conf, String name, Progressable progress) throws IOException {

      CompressionCodecName codec = getCodec(conf);
      String extension = codec.getExtension() + ".parquet";
      Path file = getDefaultWorkFile(conf, name, extension);

      try {
        realWriter = (ParquetRecordWriter<V>) realOutputFormat.getRecordWriter(conf, file, codec);
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      try {
        realWriter.close(null);
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public void write(Void key, V value) throws IOException {
      try {
        realWriter.write(key, value);
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }
  }
}
