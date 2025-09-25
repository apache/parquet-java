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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.apache.parquet.proto.test.TestProtobuf;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
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
  public void testCatCommandWithSimpleReaderConfig() throws Exception {
    File regularFile = parquetFile();

    Configuration conf = new Configuration();
    conf.setBoolean("parquet.enable.simple-reader", true);

    CatCommand cmd = new CatCommand(createLogger(), 5);
    cmd.sourceFiles = Arrays.asList(regularFile.getAbsolutePath());
    cmd.setConf(conf);

    int result = cmd.run();
    Assert.assertEquals(0, result);
  }

  @Test
  public void testCatCommandWithHyphenatedFieldNames() throws Exception {
    File hyphenFile = new File(getTempFolder(), "hyphenated_fields.parquet");
    writeParquetWithHyphenatedFields(hyphenFile);

    CatCommand cmd = new CatCommand(createLogger(), 1);
    cmd.sourceFiles = Arrays.asList(hyphenFile.getAbsolutePath());
    cmd.setConf(new Configuration());

    int result = cmd.run();
    Assert.assertEquals(0, result);
  }

  private static void writeParquetWithHyphenatedFields(File file) throws IOException {
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.INT32)
        .named("order_id")
        .required(PrimitiveTypeName.BINARY)
        .named("customer-name")
        .required(PrimitiveTypeName.BINARY)
        .named("product-category")
        .required(PrimitiveTypeName.DOUBLE)
        .named("sale-amount")
        .required(PrimitiveTypeName.BINARY)
        .named("region")
        .named("SalesRecord");

    SimpleGroupFactory factory = new SimpleGroupFactory(schema);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.getAbsolutePath()))
        .withType(schema)
        .build()) {

      Group record1 = factory.newGroup()
          .append("order_id", 1001)
          .append("customer-name", "John Smith")
          .append("product-category", "Electronics")
          .append("sale-amount", 299.99)
          .append("region", "North");
      writer.write(record1);

      Group record2 = factory.newGroup()
          .append("order_id", 1002)
          .append("customer-name", "Jane Doe")
          .append("product-category", "Home-Garden")
          .append("sale-amount", 149.50)
          .append("region", "South");
      writer.write(record2);
    }
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
