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
package org.apache.parquet.cli.commands;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ColumnSizeCommandTest extends ParquetFileTest {

  private final int numRecord = 10000;
  private ColumnSizeCommand command = new ColumnSizeCommand(createLogger());
  private Configuration conf = new Configuration();

  @Test
  public void testColumnSizeCommand() throws IOException {
    File file = parquetFile();
    ColumnSizeCommand command = new ColumnSizeCommand(createLogger());
    command.target = file.getAbsolutePath();
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }

  @Test
  public void testColumnSize() throws Exception {
    String inputFile = createParquetFile();
    Map<String, Long> columnSizeInBytes = command.getColumnSizeInBytes(new Path(inputFile));
    assertEquals(columnSizeInBytes.size(), 2);
    assertTrue(columnSizeInBytes.get("DocId") > columnSizeInBytes.get("Num"));
    Map<String, Float> columnRatio = command.getColumnRatio(columnSizeInBytes);
    assertTrue(columnRatio.get("DocId") > columnRatio.get("Num"));
  }

  private String createParquetFile() throws IOException {
    MessageType schema = new MessageType("schema",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(REQUIRED, INT32, "Num"));

    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());

    String file = randomParquetFile().getAbsolutePath();
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(file)).withConf(conf);
    Random rnd = new Random();
    try (ParquetWriter writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("DocId", rnd.nextLong());
        g.add("Num", rnd.nextInt());
        writer.write(g);
      }
    }

    return file;
  }
}
