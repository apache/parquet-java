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



  Configuration conf = new Configuration();
  String writeSchema = "message example {\n" +
      "required int32 line;\n" +
      "required binary content;\n" +
      "}";
  long expectPoolSize;

  /**
   * A fake class to simplify the construction and close method of InternalParquetRecordWriter
   * for testing.
   */
  class MyInternalParquetRecordWriter extends InternalParquetRecordWriter {
    private MemoryManager memoryManager;

    MyInternalParquetRecordWriter(int rowGroupSize, WriteSupport writeSupport,
                                  WriteSupport.WriteContext writeContext,
                                  CodecFactory.BytesCompressor compressor) {
      super(null, writeSupport, writeContext.getSchema(),
          writeContext.getExtraMetaData(), rowGroupSize, 1000, compressor, 1000, true, false,
          ParquetProperties.WriterVersion.PARQUET_1_0, writeContext.getMemoryManager());
      memoryManager = writeContext.getMemoryManager();
    }

    @Override
    public void close() {
      memoryManager.removeWriter(this);
    }
  }

  @Before
  public void setUp() {
    GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema),conf);
  }

  @Test
  public void testMemoryManager() throws Exception {
    expectPoolSize = Math.round(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax
        () * MemoryManager.DEFAULT_MEMORY_POOL_RATIO);

    //Verify the adjusted rowGroupSize of writers
    int rowGroupSize = (int) Math.floor(expectPoolSize / 2);

    MyInternalParquetRecordWriter writer1 = createWriter(rowGroupSize);
    Assert.assertEquals("unmatched rowGroupSize", rowGroupSize, writer1.getRowGroupSizeThreshold());

    MyInternalParquetRecordWriter writer2 = createWriter(rowGroupSize);
    Assert.assertEquals("unmatched rowGroupSize", rowGroupSize, writer2.getRowGroupSizeThreshold());

    MyInternalParquetRecordWriter writer3 = createWriter(rowGroupSize);
    Assert.assertEquals("unmatched rowGroupSize", (int) Math.floor(expectPoolSize / 3),
        writer3.getRowGroupSizeThreshold(), 1);

    writer1.close();
    Assert.assertEquals("unmatched rowGroupSize", rowGroupSize, writer2.getRowGroupSizeThreshold());
    Assert.assertEquals("unmatched rowGroupSize", rowGroupSize, writer3.getRowGroupSizeThreshold());

    writer2.close();
    Assert.assertEquals("unmatched rowGroupSize", rowGroupSize, writer3.getRowGroupSizeThreshold());

    writer3.close();
  }

  private MyInternalParquetRecordWriter createWriter(int rowGroupSize) {
    WriteSupport writeSupport = new GroupWriteSupport();
    WriteSupport.WriteContext writeContext = writeSupport.init(conf);
    CodecFactory codecFactory = new CodecFactory(conf);
    CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(
        CompressionCodecName.UNCOMPRESSED, 1000);

    //Verify the memory pool
    Assert.assertEquals("memory pool size is incorrect.", expectPoolSize,
        writeContext.getMemoryManager().getTotalMemoryPool());

    return new MyInternalParquetRecordWriter(rowGroupSize, writeSupport, writeContext, compressor);
  }
}
