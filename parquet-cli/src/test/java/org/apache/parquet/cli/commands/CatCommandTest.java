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

import com.google.protobuf.Message;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.apache.parquet.proto.test.TestProtobuf;
import org.junit.Assert;
import org.junit.Test;

public class CatCommandTest extends ParquetFileTest {
  @Test
  public void testCatCommand() throws IOException {
    File file = parquetFile();
    CatCommand command = new CatCommand(createLogger(), 0);
    command.sourceFiles = Arrays.asList(file.getAbsolutePath());
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }

  @Test
  public void testCatCommandWithMultipleInput() throws IOException {
    File file = parquetFile();
    CatCommand command = new CatCommand(createLogger(), 0);
    command.sourceFiles = Arrays.asList(file.getAbsolutePath(), file.getAbsolutePath());
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }

  @Test
  public void testCatCommandWithSpecificColumns() throws IOException {
    File file = parquetFile();
    CatCommand command = new CatCommand(createLogger(), 0);
    command.sourceFiles = Arrays.asList(file.getAbsolutePath());
    command.columns = Arrays.asList(INT32_FIELD, INT64_FIELD);
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCatCommandWithInvalidColumn() throws IOException {
    File file = parquetFile();
    CatCommand command = new CatCommand(createLogger(), 0);
    command.sourceFiles = Arrays.asList(file.getAbsolutePath());
    command.columns = Arrays.asList("invalid_field");
    command.setConf(new Configuration());
    command.run();
  }

  @Test
  public void testCatCommandProtoParquetAutoDetected() throws Exception {
    File protoFile = new File(getTempFolder(), "proto_someevent.parquet");
    writeProtoParquet(protoFile);

    CatCommand cmd = new CatCommand(createLogger(), 0);
    cmd.sourceFiles = Arrays.asList(protoFile.getAbsolutePath());
    cmd.setConf(new Configuration());

    int result = cmd.run();
    Assert.assertEquals(0, result);
  }

  @Test
  public void testCatCommandProtoParquetSucceedsWithAutoDetection() throws Exception {
    File protoFile = new File(getTempFolder(), "proto_someevent.parquet");
    writeProtoParquet(protoFile);

    CatCommand cmd = new CatCommand(createLogger(), 0);
    cmd.sourceFiles = Arrays.asList(protoFile.getAbsolutePath());
    cmd.setConf(new Configuration());

    int result = cmd.run();
    Assert.assertEquals(0, result);
  }

  private static void writeProtoParquet(File file) throws Exception {
    TestProtobuf.RepeatedIntMessage.Builder b = TestProtobuf.RepeatedIntMessage.newBuilder()
        .addRepeatedInt(1)
        .addRepeatedInt(2)
        .addRepeatedInt(3);

    try (ProtoParquetWriter<Message> w =
        new ProtoParquetWriter<>(new Path(file.getAbsolutePath()), TestProtobuf.RepeatedIntMessage.class)) {
      w.write(b.build());
    }
  }
}
