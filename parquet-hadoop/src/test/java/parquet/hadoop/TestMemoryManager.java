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

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import parquet.column.ParquetProperties;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageTypeParser;

import java.lang.management.ManagementFactory;

/**
 * Verify MemoryManager could adjust its writers' allocated memory size.
 */
public class TestMemoryManager {

  WriteSupport.WriteContext writeContext;
  CodecFactory.BytesCompressor compressor;
  WriteSupport writeSupport;

  /**
   * A fake class to simplify the construction and close method of InternalParquetRecordWriter
   * for testing.
   */
  class MyInternalParquetRecordWriter extends InternalParquetRecordWriter {
    MyInternalParquetRecordWriter(int rowGroupSize) {
      super(null, writeSupport, writeContext.getSchema(),
          writeContext.getExtraMetaData(), rowGroupSize, 1000, compressor, 1000, true, false,
          ParquetProperties.WriterVersion.PARQUET_1_0);
    }

    @Override
    public void close() {
      MemoryManager.getMemoryManager().removeWriter(this);
    }
  }

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    String writeSchema = "message example {\n" +
        "required int32 line;\n" +
        "required binary content;\n" +
        "}";
    GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema),conf);
    writeSupport = new GroupWriteSupport();
    writeContext = writeSupport.init(conf);
    CodecFactory codecFactory = new CodecFactory(conf);
    compressor = codecFactory.getCompressor(CompressionCodecName.UNCOMPRESSED, 1000);
  }

  @Test
  public void testMemoryManager() throws Exception {
    //Verify the memory pool
    MemoryManager memoryManager = MemoryManager.getMemoryManager();
    long expectPoolSize = Math.round(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax
        () * MemoryManager.MEMORY_POOL_PERCENTAGE);
    Assert.assertEquals("memory pool size is incorrect.", expectPoolSize,
        memoryManager.getTotalMemoryPool());

    //Verify the adjusted rowGroupSize of writers
    int rowGroupSize = (int) Math.floor(expectPoolSize / 2);

    MyInternalParquetRecordWriter writer1 = new MyInternalParquetRecordWriter(rowGroupSize);
    Assert.assertEquals("unmatched rowGroupSize", rowGroupSize, writer1.getRowGroupSize());

    MyInternalParquetRecordWriter writer2 = new MyInternalParquetRecordWriter(rowGroupSize);
    Assert.assertEquals("unmatched rowGroupSize", rowGroupSize, writer2.getRowGroupSize());

    MyInternalParquetRecordWriter writer3 = new MyInternalParquetRecordWriter(rowGroupSize);
    Assert.assertEquals("unmatched rowGroupSize", (int) Math.floor(expectPoolSize / 3),
        writer3.getRowGroupSize(), 1);

    writer1.close();
    Assert.assertEquals("unmatched rowGroupSize", rowGroupSize, writer2.getRowGroupSize());
    Assert.assertEquals("unmatched rowGroupSize", rowGroupSize, writer3.getRowGroupSize());

    writer2.close();
    Assert.assertEquals("unmatched rowGroupSize", rowGroupSize, writer3.getRowGroupSize());

    writer3.close();
  }
}
