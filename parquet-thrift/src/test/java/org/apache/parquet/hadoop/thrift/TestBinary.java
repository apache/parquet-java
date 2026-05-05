/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.thrift;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.thrift.ThriftParquetReader;
import org.apache.parquet.thrift.ThriftParquetWriter;
import org.apache.parquet.thrift.test.binary.StringAndBinary;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestBinary {
  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testBinary() throws IOException {
    StringAndBinary expected = new StringAndBinary("test", ByteBuffer.wrap(new byte[] {-123, 20, 33}));
    File temp = tempDir.newFile(UUID.randomUUID().toString());
    temp.deleteOnExit();
    temp.delete();

    Path path = new Path(temp.getPath());

    ThriftParquetWriter<StringAndBinary> writer =
        new ThriftParquetWriter<StringAndBinary>(path, StringAndBinary.class, CompressionCodecName.SNAPPY);
    writer.write(expected);
    writer.close();

    ParquetReader<StringAndBinary> reader = ThriftParquetReader.<StringAndBinary>build(path)
        .withThriftClass(StringAndBinary.class)
        .build();

    StringAndBinary record = reader.read();
    reader.close();

    assertSchema(ParquetFileReader.readFooter(new Configuration(), path));
    assertEquals("Should match after serialization round trip", expected, record);
  }

  private void assertSchema(ParquetMetadata parquetMetadata) {
    List<Type> fields = parquetMetadata.getFileMetaData().getSchema().getFields();
    assertEquals(2, fields.size());
    assertEquals(
        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(OriginalType.UTF8)
            .id(1)
            .named("s"),
        fields.get(0));
    assertEquals(
        Types.required(PrimitiveType.PrimitiveTypeName.BINARY).id(2).named("b"), fields.get(1));
  }
}
