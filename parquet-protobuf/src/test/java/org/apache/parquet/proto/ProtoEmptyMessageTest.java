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
package org.apache.parquet.proto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.proto.test.Trees;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

/**
 * Fields typed as an EMPTY proto message cannot map to a parquet group (parquet forbids empty
 * groups, so writer construction used to fail with an {@code InvalidSchemaException}). They are
 * now terminated as proto bytes, like recursion beyond maxRecursion, which also keeps the field's
 * presence observable (null vs an empty byte array).
 * <p>
 * The written files are read back both with {@link ProtoParquetReader} (which parses the bytes
 * back into the message) and with parquet-hadoop's protobuf-agnostic {@link GroupReadSupport},
 * to show that a reader without protobuf knowledge sees plain, unannotated binary columns.
 */
public class ProtoEmptyMessageTest {

  private static Path write(boolean specsCompliant, Message... messages) throws IOException {
    Path file = TestUtils.someTemporaryFilePath();
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, specsCompliant);
    try (ParquetWriter<Message> writer = ProtoParquetWriter.<Message>builder(file)
        .withMessage(messages[0].getClass())
        .withConf(conf)
        .build()) {
      for (Message message : messages) {
        writer.write(message);
      }
    }
    return file;
  }

  /** Reads with parquet-hadoop's generic {@link GroupReadSupport}, which knows nothing about protobuf. */
  private static List<Group> readWithGenericReader(Path file) throws IOException {
    List<Group> rows = new ArrayList<>();
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), file).build()) {
      for (Group group = reader.read(); group != null; group = reader.read()) {
        rows.add(group);
      }
    }
    return rows;
  }

  private static MessageType readFooterSchema(Path file) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new Configuration(), file)) {
      return reader.getFileMetaData().getSchema();
    }
  }

  @Test
  public void emptyMessageFieldsAreUnannotatedBinaryColumns() throws Exception {
    // What a reader without protobuf knowledge sees in the footer: plain BINARY columns with no
    // logical type, keeping the field's repetition (or the standard LIST/MAP wrappers).
    MessageType schema = readFooterSchema(write(true, Trees.StubBox.getDefaultInstance()));

    assertThat(schema)
        .isEqualTo(MessageTypeParser.parseMessageType("message Trees.StubBox {\n"
            + "  optional binary stub = 1;\n"
            + "  optional group stubs (LIST) = 2 {\n"
            + "    repeated group list {\n"
            + "      required binary element;\n"
            + "    }\n"
            + "  }\n"
            + "  optional group stub_map (MAP) = 3 {\n"
            + "    repeated group key_value {\n"
            + "      required binary key (STRING);\n"
            + "      optional binary value;\n"
            + "    }\n"
            + "  }\n"
            + "  optional binary name (STRING) = 4;\n"
            + "}"));

    PrimitiveType stub = schema.getType("stub").asPrimitiveType();
    assertThat(stub.getPrimitiveTypeName()).isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
    assertThat(stub.getLogicalTypeAnnotation())
        .as("no logical type: the bytes are opaque to non-protobuf readers")
        .isNull();

    PrimitiveType element = schema.getType("stubs", "list", "element").asPrimitiveType();
    assertThat(element.getPrimitiveTypeName()).isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
    assertThat(element.getLogicalTypeAnnotation()).isNull();

    PrimitiveType value = schema.getType("stub_map", "key_value", "value").asPrimitiveType();
    assertThat(value.getPrimitiveTypeName()).isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
    assertThat(value.getLogicalTypeAnnotation()).isNull();
  }

  @Test
  public void emptyMessageFieldsAreUnannotatedBinaryColumnsOldStyle() throws Exception {
    MessageType schema = readFooterSchema(write(false, Trees.StubBox.getDefaultInstance()));

    PrimitiveType stubs = schema.getType("stubs").asPrimitiveType();
    assertThat(stubs.getRepetition())
        .as("old style keeps the repeated field itself")
        .isEqualTo(PrimitiveType.Repetition.REPEATED);
    assertThat(stubs.getPrimitiveTypeName()).isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
    assertThat(stubs.getLogicalTypeAnnotation()).isNull();
  }

  @Test
  public void emptyMessageFieldsWriteAsBytes() throws Exception {
    Trees.StubBox box = Trees.StubBox.newBuilder()
        .setStub(Trees.Stub.getDefaultInstance())
        .addStubs(Trees.Stub.getDefaultInstance())
        .addStubs(Trees.Stub.getDefaultInstance())
        .putStubMap("k", Trees.Stub.getDefaultInstance())
        .setName("x")
        .build();

    Group row = readWithGenericReader(write(true, box)).get(0);
    assertThat(row.getBinary("stub", 0).length())
        .as("optional empty message present as zero bytes")
        .isEqualTo(0);
    assertThat(row.getGroup("stubs", 0).getFieldRepetitionCount("list"))
        .as("repeated empty messages keep their cardinality")
        .isEqualTo(2);
    Group entry = row.getGroup("stub_map", 0).getGroup("key_value", 0);
    assertThat(entry.getString("key", 0)).as("map keys stay typed").isEqualTo("k");
    assertThat(entry.getBinary("value", 0).length())
        .as("map value is zero bytes")
        .isEqualTo(0);
    assertThat(row.getString("name", 0)).isEqualTo("x");
  }

  @Test
  public void emptyMessagePresenceRoundTrips() throws Exception {
    Trees.StubBox with = Trees.StubBox.newBuilder()
        .setStub(Trees.Stub.getDefaultInstance())
        .build();
    Trees.StubBox without = Trees.StubBox.getDefaultInstance();

    List<Group> rows = readWithGenericReader(write(true, with, without));
    assertThat(rows.get(0).getFieldRepetitionCount("stub"))
        .as("set empty message is present")
        .isEqualTo(1);
    assertThat(rows.get(1).getFieldRepetitionCount("stub"))
        .as("unset empty message is null")
        .isEqualTo(0);
  }

  @Test
  public void emptyMessageFieldsWriteAsBytesOldStyle() throws Exception {
    Trees.StubBox box = Trees.StubBox.newBuilder()
        .addStubs(Trees.Stub.getDefaultInstance())
        .addStubs(Trees.Stub.getDefaultInstance())
        .build();

    Group row = readWithGenericReader(write(false, box)).get(0);
    assertThat(row.getFieldRepetitionCount("stubs"))
        .as("repeated empty messages keep their cardinality")
        .isEqualTo(2);
  }

  @Test
  public void emptyMessageFieldsRoundTripThroughProtoReader() throws Exception {
    Trees.StubBox box = Trees.StubBox.newBuilder()
        .setStub(Trees.Stub.getDefaultInstance())
        .addStubs(Trees.Stub.getDefaultInstance())
        .addStubs(Trees.Stub.getDefaultInstance())
        .putStubMap("k", Trees.Stub.getDefaultInstance())
        .setName("x")
        .build();
    Trees.StubBox without = Trees.StubBox.newBuilder().setName("y").build();

    assertThat(TestUtils.readMessages(write(true, box, without), Trees.StubBox.class))
        .as("ProtoParquetReader parses the proto bytes back, presence included")
        .containsExactly(box, without);
  }

  @Test
  public void emptyMessageFieldsRoundTripThroughProtoReaderOldStyle() throws Exception {
    Trees.StubBox box = Trees.StubBox.newBuilder()
        .setStub(Trees.Stub.getDefaultInstance())
        .addStubs(Trees.Stub.getDefaultInstance())
        .addStubs(Trees.Stub.getDefaultInstance())
        .putStubMap("k", Trees.Stub.getDefaultInstance())
        .build();

    assertThat(TestUtils.readMessages(write(false, box), Trees.StubBox.class))
        .containsExactly(box);
  }

  @Test
  public void truncatedRecursionRoundTripsThroughProtoReader() throws Exception {
    // the same binary-to-message read path serves recursion truncated at maxRecursion
    Trees.BinaryTree.Builder tree = Trees.BinaryTree.newBuilder();
    Trees.BinaryTree.Builder cursor = tree;
    for (int i = 0; i < 6; i++) {
      cursor.getValueBuilder().setTypeUrl("level-" + i);
      cursor = cursor.getLeftBuilder();
    }
    Path file = TestUtils.someTemporaryFilePath();
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoSchemaConverter.setMaxRecursion(conf, 2);
    try (ParquetWriter<Message> writer = ProtoParquetWriter.<Message>builder(file)
        .withMessage(Trees.BinaryTree.class)
        .withConf(conf)
        .build()) {
      writer.write(tree.build());
    }

    assertThat(TestUtils.readMessages(file, Trees.BinaryTree.class)).containsExactly(tree.build());
  }

  @Test
  public void emptyRootMessageStillRejected() {
    // the root message itself cannot be terminated as bytes - there is no field to hold them
    assertThatThrownBy(() -> write(true, Trees.Stub.getDefaultInstance()))
        .isInstanceOf(InvalidSchemaException.class)
        .hasMessageContaining("Cannot write a schema with an empty group");
  }
}
