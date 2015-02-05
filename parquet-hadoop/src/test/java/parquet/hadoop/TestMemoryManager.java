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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageTypeParser;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Set;

/**
 * Verify MemoryManager could adjust its writers' allocated memory size.
 */
public class TestMemoryManager {

  Configuration conf = new Configuration();
  String writeSchema = "message example {\n" +
      "required int32 line;\n" +
      "required binary content;\n" +
      "}";
  long expectPoolSize;
  int rowGroupSize;
  ParquetOutputFormat parquetOutputFormat;
  CompressionCodecName codec;

  @Before
  public void setUp() {
    GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema),conf);
    expectPoolSize = Math.round(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax
        () * MemoryManager.DEFAULT_MEMORY_POOL_RATIO);
    rowGroupSize = (int) Math.floor(expectPoolSize / 2);
    conf.setInt(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize);
    codec = CompressionCodecName.UNCOMPRESSED;
  }

  @After
  public void tearDown() throws Exception{
    FileUtils.deleteDirectory(new File("target/test"));
  }

  @Test
  public void testMemoryManager() throws Exception {
    //Verify the adjusted rowGroupSize of writers
    RecordWriter writer1 = createWriter(1);
    verifyRowGroupSize(rowGroupSize);

    RecordWriter writer2 = createWriter(2);
    verifyRowGroupSize(rowGroupSize);

    RecordWriter writer3 = createWriter(3);
    verifyRowGroupSize((int) Math.floor(expectPoolSize / 3));

    writer1.close(null);
    verifyRowGroupSize(rowGroupSize);

    writer2.close(null);
    verifyRowGroupSize(rowGroupSize);

    writer3.close(null);

    //Verify the memory pool
    Assert.assertEquals("memory pool size is incorrect.", expectPoolSize,
        parquetOutputFormat.getMemoryManager().getTotalMemoryPool());
  }

  private RecordWriter createWriter(int index) throws Exception{
    Path file = new Path("target/test/", "parquet" + index);
    parquetOutputFormat = new ParquetOutputFormat(new GroupWriteSupport());
    return parquetOutputFormat.getRecordWriter(conf, file, codec);
  }

  private void verifyRowGroupSize(int expectRowGroupSize) {
    Set<InternalParquetRecordWriter> writers = parquetOutputFormat.getMemoryManager()
        .getWriterList().keySet();
    for (InternalParquetRecordWriter writer : writers) {
      Assert.assertEquals("wrong rowGroupSize", expectRowGroupSize,
          writer.getRowGroupSizeThreshold(), 1);
    }
  }
}
