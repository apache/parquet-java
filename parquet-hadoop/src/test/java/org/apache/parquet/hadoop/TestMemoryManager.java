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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verify MemoryManager could adjust its writers' allocated memory size.
 */
public class TestMemoryManager {

  Configuration conf = new Configuration();
  String writeSchema = "message example {\n" + "required int32 line;\n" + "required binary content;\n" + "}";
  long expectedPoolSize;
  ParquetOutputFormat parquetOutputFormat;
  int counter = 0;

  @Before
  public void setUp() throws Exception {
    parquetOutputFormat = new ParquetOutputFormat(new GroupWriteSupport());

    GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema), conf);
    expectedPoolSize = Math.round((double)
            ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax()
        * MemoryManager.DEFAULT_MEMORY_POOL_RATIO);

    long rowGroupSize = expectedPoolSize / 2;
    conf.setLong(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize);

    // the memory manager is not initialized until a writer is created
    createWriter(0).close(null);
  }

  @Test
  public void testMemoryManagerUpperLimit() {
    // Verify the memory pool size
    // this value tends to change a little between setup and tests, so this
    // validates that it is within 15% of the expected value
    long poolSize = ParquetOutputFormat.getMemoryManager().getTotalMemoryPool();
    assertThat(Math.abs(expectedPoolSize - poolSize))
        .as("Pool size should be within 15% of the expected value" + " (expected = " + expectedPoolSize
            + " actual = " + poolSize + ")")
        .isLessThan((long) (expectedPoolSize * 0.15));
  }

  @Test
  public void testMemoryManager() throws Exception {
    long poolSize = ParquetOutputFormat.getMemoryManager().getTotalMemoryPool();
    long rowGroupSize = poolSize / 2;
    conf.setLong(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize);

    assertThat(2 * rowGroupSize).as("Pool should hold 2 full row groups").isLessThanOrEqualTo(poolSize);
    assertThat(poolSize).as("Pool should not hold 3 full row groups").isLessThan(3 * rowGroupSize);

    assertThat(getTotalAllocation()).as("Allocations should start out at 0").isEqualTo(0);

    RecordWriter writer1 = createWriter(1);
    assertThat(getTotalAllocation())
        .as("Allocations should never exceed pool size")
        .isLessThanOrEqualTo(poolSize);
    assertThat(getTotalAllocation())
        .as("First writer should be limited by row group size")
        .isEqualTo(rowGroupSize);

    RecordWriter writer2 = createWriter(2);
    assertThat(getTotalAllocation())
        .as("Allocations should never exceed pool size")
        .isLessThanOrEqualTo(poolSize);
    assertThat(getTotalAllocation())
        .as("Second writer should be limited by row group size")
        .isEqualTo(2 * rowGroupSize);

    RecordWriter writer3 = createWriter(3);
    assertThat(getTotalAllocation())
        .as("Allocations should never exceed pool size")
        .isLessThanOrEqualTo(poolSize);

    writer1.close(null);
    assertThat(getTotalAllocation())
        .as("Allocations should never exceed pool size")
        .isLessThanOrEqualTo(poolSize);
    assertThat(getTotalAllocation())
        .as("Allocations should be increased to the row group size")
        .isEqualTo(2 * rowGroupSize);

    writer2.close(null);
    assertThat(getTotalAllocation())
        .as("Allocations should never exceed pool size")
        .isLessThanOrEqualTo(poolSize);
    assertThat(getTotalAllocation())
        .as("Allocations should be increased to the row group size")
        .isEqualTo(rowGroupSize);

    writer3.close(null);
    assertThat(getTotalAllocation())
        .as("Allocations should be increased to the row group size")
        .isEqualTo(0);
  }

  @Test
  public void testReallocationCallback() throws Exception {
    // validate assumptions
    long poolSize = ParquetOutputFormat.getMemoryManager().getTotalMemoryPool();
    long rowGroupSize = poolSize / 2;
    conf.setLong(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize);

    assertThat(2 * rowGroupSize).as("Pool should hold 2 full row groups").isLessThanOrEqualTo(poolSize);
    assertThat(poolSize).as("Pool should not hold 3 full row groups").isLessThan(3 * rowGroupSize);

    Runnable callback = () -> counter++;

    // first-time registration should succeed
    ParquetOutputFormat.getMemoryManager().registerScaleCallBack("increment-test-counter", callback);

    assertThatThrownBy(() -> ParquetOutputFormat.getMemoryManager()
            .registerScaleCallBack("increment-test-counter", callback))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The callBackName increment-test-counter is duplicated and has been registered already.");

    // hit the limit once and clean up
    RecordWriter writer1 = createWriter(1);
    RecordWriter writer2 = createWriter(2);
    RecordWriter writer3 = createWriter(3);
    writer1.close(null);
    writer2.close(null);
    writer3.close(null);

    // Verify Callback mechanism
    assertThat(counter).as("Allocations should be adjusted once").isEqualTo(1);
    assertThat(ParquetOutputFormat.getMemoryManager().getScaleCallBacks())
        .as("Should not allow duplicate callbacks")
        .hasSize(1);
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private RecordWriter createWriter(int index) throws Exception {
    File file = temp.newFile(String.valueOf(index) + ".parquet");
    if (!file.delete()) {
      throw new RuntimeException("Could not delete file: " + file);
    }
    RecordWriter writer =
        parquetOutputFormat.getRecordWriter(conf, new Path(file.toString()), CompressionCodecName.UNCOMPRESSED);

    return writer;
  }

  private long getTotalAllocation() {
    Set<InternalParquetRecordWriter<?>> writers =
        ParquetOutputFormat.getMemoryManager().getWriterList().keySet();
    long total = 0;
    for (InternalParquetRecordWriter<?> writer : writers) {
      total += writer.getRowGroupSizeThreshold();
    }
    return total;
  }
}
