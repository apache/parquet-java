/**
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
package parquet.hadoop.thrift;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.thrift.ThriftParquetReader;
import parquet.thrift.ThriftParquetWriter;
import parquet.thrift.test.binary.StringAndBinary;

public class TestBinary {
  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testBinary() throws IOException {
    StringAndBinary expected = new StringAndBinary("test",
        ByteBuffer.wrap(new byte[] { -123, 20, 33 }));
    File temp = tempDir.newFile(UUID.randomUUID().toString());
    temp.deleteOnExit();
    temp.delete();

    Path path = new Path(temp.getPath());

    ThriftParquetWriter<StringAndBinary> writer =
        new ThriftParquetWriter<StringAndBinary>(
            path, StringAndBinary.class, CompressionCodecName.SNAPPY);
    writer.write(expected);
    writer.close();

    ParquetReader<StringAndBinary> reader = ThriftParquetReader.<StringAndBinary>
        build(path)
        .withThriftClass(StringAndBinary.class)
        .build();
    StringAndBinary record = reader.read();
    reader.close();

    Assert.assertEquals("Should match after serialization round trip",
        expected, record);
  }
}
