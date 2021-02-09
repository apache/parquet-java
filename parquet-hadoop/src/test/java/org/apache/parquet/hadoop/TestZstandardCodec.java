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

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.NoPool;
import com.github.luben.zstd.RecyclingBufferPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.codec.ZstandardCodec;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Random;

public class TestZstandardCodec {

  private final Path inputPath = new Path("src/test/java/org/apache/parquet/hadoop/example/TestInputOutputFormat.java");

  @Test
  public void testZstdCodec() throws IOException {
    ZstandardCodec codec = new ZstandardCodec();
    Configuration conf = new Configuration();
    boolean[] pools = {false, true};
    int[] levels = {1, 4, 7, 10, 13, 16, 19, 22};
    int[] dataSizes = {0, 1, 10, 1024, 1024 * 1024};

    for (boolean pool: pools) {
      for (int i = 0; i < levels.length; i++) {
        conf.setBoolean(ZstandardCodec.PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED, pool);
        conf.setInt(ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL, levels[i]);
        codec.setConf(conf);
        for (int j = 0; j < dataSizes.length; j++) {
          testZstd(codec, dataSizes[j]);
        }
      }
    }
  }

  private void testZstd(ZstandardCodec codec, int dataSize) throws IOException {
    byte[] data = new byte[dataSize];
    (new Random()).nextBytes(data);
    BytesInput compressedData = compress(codec,  BytesInput.from(data));
    BytesInput decompressedData = decompress(codec, compressedData, data.length);
    Assert.assertArrayEquals(data, decompressedData.toByteArray());
  }

  private BytesInput compress(ZstandardCodec codec, BytesInput bytes) throws IOException {
    ByteArrayOutputStream compressedOutBuffer = new ByteArrayOutputStream((int)bytes.size());
    CompressionOutputStream cos = codec.createOutputStream(compressedOutBuffer, null);
    bytes.writeAllTo(cos);
    cos.close();
    return BytesInput.from(compressedOutBuffer);
  }

  private BytesInput decompress(ZstandardCodec codec, BytesInput bytes, int uncompressedSize) throws IOException {
    BytesInput decompressed;
    InputStream is = codec.createInputStream(bytes.toInputStream(), null);
    decompressed = BytesInput.from(is, uncompressedSize);
    is.close();
    return decompressed;
  }

  /**
   *  This test is to verify that the properties are passed through from the config to the codec. 
   */
  @Test
  public void testZstdConfWithMr() throws Exception {
    long fileSizeLowLevel = runMrWithConf(1);
    // Clear the cache so that a new codec can be created with new configuration
    CodecFactory.CODEC_BY_NAME.clear();
    long fileSizeHighLevel = runMrWithConf(22);
    Assert.assertTrue(fileSizeLowLevel > fileSizeHighLevel);
  }

  private long runMrWithConf(int level) throws Exception {
    JobConf jobConf = new JobConf();
    Configuration conf = new Configuration();
    jobConf.setInt(ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL, level);
    jobConf.setInt(ZstandardCodec.PARQUET_COMPRESS_ZSTD_WORKERS, 4);
    Path path = new Path(Files.createTempDirectory("zstd" + level).toAbsolutePath().toString());
    RunningJob mapRedJob = runMapReduceJob(CompressionCodecName.ZSTD, jobConf, conf, path);
    Assert.assertTrue(mapRedJob.isSuccessful());
    return getFileSize(path, conf);
  }

  private RunningJob runMapReduceJob(CompressionCodecName codec, JobConf jobConf, Configuration conf, Path parquetPath) throws IOException, ClassNotFoundException, InterruptedException {
    String writeSchema = "message example {\n" +
      "required int32 line;\n" +
      "required binary content;\n" +
      "}";

    FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(parquetPath, true);
    jobConf.setInputFormat(TextInputFormat.class);
    TextInputFormat.addInputPath(jobConf, inputPath);
    jobConf.setNumReduceTasks(0);
    jobConf.setOutputFormat(DeprecatedParquetOutputFormat.class);
    DeprecatedParquetOutputFormat.setCompression(jobConf, codec);
    DeprecatedParquetOutputFormat.setOutputPath(jobConf, parquetPath);
    DeprecatedParquetOutputFormat.setWriteSupportClass(jobConf, GroupWriteSupport.class);
    GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema), jobConf);

    jobConf.setMapperClass(TestZstandardCodec.DumpMapper.class);
    return JobClient.runJob(jobConf);
  }

  private long getFileSize(Path parquetPath, Configuration conf) throws IOException {
    for (FileStatus file : parquetPath.getFileSystem(conf).listStatus(parquetPath)) {
      if (file.getPath().getName().endsWith(".parquet")) {
        return file.getLen();
      }
    }
    return -1;
  }

  public static class DumpMapper implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Void, Group> {
    private SimpleGroupFactory factory;

    public void configure(JobConf job) {
      factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(job));
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Void, Group> outputCollector, Reporter reporter) throws IOException {
      Group group = factory.newGroup()
        .append("line", (int) key.get())
        .append("content", value.toString());
      outputCollector.collect(null, group);
    }

    @Override
    public void close() {
    }
  }
}
