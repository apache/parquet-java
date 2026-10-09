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

import com.google.protobuf.ListValue;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.proto.test.Trees;
import org.junit.jupiter.api.Test;

/**
 * End-to-end write tests for recursive messages that get truncated to proto bytes at
 * {@code maxRecursion} depth. Unlike the RecordConsumer-mock tests in {@link
 * ProtoWriteSupportTest}, these write through a real {@code MessageColumnIO}, which validates the
 * emitted record structure against the schema - the case that used to fail with a
 * {@code ClassCastException} (specs-compliant mode) or a repetition violation (old style) when the
 * truncated recursive field is repeated or a map.
 */
public class ProtoRecursionTruncationTest {

  /** A WideTree of the given depth with {@code branching} children at every level. */
  private static Trees.WideTree wideTree(int depth, int branching) {
    Trees.WideTree.Builder node = Trees.WideTree.newBuilder();
    node.getValueBuilder().setTypeUrl("level-" + depth);
    if (depth > 0) {
      for (int i = 0; i < branching; i++) {
        node.addChildren(wideTree(depth - 1, branching));
      }
    }
    return node.build();
  }

  /** A Struct nested through its map values to the given depth. */
  private static Struct deepStruct(int depth) {
    Struct.Builder struct = Struct.newBuilder();
    if (depth > 0) {
      struct.putFields(
          "level-" + depth,
          Value.newBuilder().setStructValue(deepStruct(depth - 1)).build());
    } else {
      struct.putFields("leaf", Value.newBuilder().setStringValue("x").build());
    }
    return struct.build();
  }

  private static Path write(Message message, boolean specsCompliant, int maxRecursion) throws IOException {
    Path file = TestUtils.someTemporaryFilePath();
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, specsCompliant);
    ProtoSchemaConverter.setMaxRecursion(conf, maxRecursion);
    try (ParquetWriter<Message> writer = ProtoParquetWriter.<Message>builder(file)
        .withMessage(message.getClass())
        .withConf(conf)
        .build()) {
      writer.write(message);
    }
    return file;
  }

  private static Group readSingleRow(Path file) throws IOException {
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), file).build()) {
      Group group = reader.read();
      assertThat(reader.read()).as("expected exactly one record").isNull();
      return group;
    }
  }

  @Test
  public void repeatedRecursionDeeperThanMaxRecursionSpecsCompliant() throws Exception {
    Trees.WideTree tree = wideTree(5, 2);
    Path file = write(tree, true, 2);

    // depth 0..2 are materialized groups; each node's children at depth 3 are truncated to a
    // LIST of proto bytes with one element per repeated message
    Group row = readSingleRow(file);
    Group level1 = row.getGroup("children", 0).getGroup("list", 0).getGroup("element", 0);
    Group level2 = level1.getGroup("children", 0).getGroup("list", 0).getGroup("element", 0);
    Group truncated = level2.getGroup("children", 0).getGroup("list", 0);
    assertThat(level2.getGroup("children", 0).getFieldRepetitionCount("list"))
        .as("both children survive at the truncation level")
        .isEqualTo(2);

    Trees.WideTree expectedSubtree = tree.getChildren(0).getChildren(0).getChildren(0);
    Trees.WideTree roundTripped =
        Trees.WideTree.parseFrom(truncated.getBinary("element", 0).getBytes());
    assertThat(roundTripped)
        .as("truncated bytes are the serialized subtree")
        .isEqualTo(expectedSubtree);
  }

  @Test
  public void repeatedRecursionDeeperThanMaxRecursionOldStyle() throws Exception {
    Trees.WideTree tree = wideTree(5, 2);
    Path file = write(tree, false, 2);

    // old style: repeated group children { ... repeated binary children; }
    Group row = readSingleRow(file);
    Group level2 = row.getGroup("children", 0).getGroup("children", 0);
    assertThat(level2.getFieldRepetitionCount("children"))
        .as("both children survive at the truncation level")
        .isEqualTo(2);

    Trees.WideTree expectedSubtree = tree.getChildren(0).getChildren(0).getChildren(0);
    Trees.WideTree roundTripped =
        Trees.WideTree.parseFrom(level2.getBinary("children", 0).getBytes());
    assertThat(roundTripped)
        .as("truncated bytes are the serialized subtree")
        .isEqualTo(expectedSubtree);
  }

  @Test
  public void mapValueRecursionDeeperThanMaxRecursionSpecsCompliant() throws Exception {
    Struct struct = deepStruct(6);
    Path file = write(struct, true, 2);

    // the MAP shape (key_value group with a typed key) is preserved; the recursion budget
    // terminates the recursive struct_value as proto bytes. On this main map path the budget
    // trips at the singular struct_value, so this pins the (unchanged) truncation shape.
    Group row = readSingleRow(file);
    Group entry = row.getGroup("fields", 0).getGroup("key_value", 0);
    assertThat(entry.getString("key", 0)).isEqualTo("level-6");

    int materializedLevels = 1;
    Group value = entry.getGroup("value", 0);
    while (!value.getType().getType("struct_value").isPrimitive()) {
      entry = value.getGroup("struct_value", 0).getGroup("fields", 0).getGroup("key_value", 0);
      assertThat(entry.getString("key", 0)).isEqualTo("level-" + (6 - materializedLevels));
      value = entry.getGroup("value", 0);
      materializedLevels++;
    }

    Struct truncated = Struct.parseFrom(value.getBinary("struct_value", 0).getBytes());
    assertThat(truncated)
        .as("truncated bytes are the serialized subtree")
        .isEqualTo(deepStruct(6 - materializedLevels));
  }

  @Test
  public void mapFieldExhaustingRecursionBudgetKeepsTypedKeys() throws Exception {
    // Nesting through list_value makes the recursion budget run out AT a map field: the whole
    // MAP - keys included - used to collapse into one optional binary in the schema, and writing
    // data through it crashed with the PrimitiveColumnIO -> GroupColumnIO ClassCastException.
    // Now the MAP structure survives and only its value is stored as proto bytes.
    Struct inner = Struct.newBuilder()
        .putFields("deep", Value.newBuilder().setStringValue("x").build())
        .build();
    Value nested = Value.newBuilder()
        .setListValue(ListValue.newBuilder()
            .addValues(Value.newBuilder()
                .setListValue(ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStructValue(inner)))))
        .build();
    Struct root = Struct.newBuilder().putFields("k", nested).build();
    Path file = write(root, true, 2);

    Group row = readSingleRow(file);
    Group entry = row.getGroup("fields", 0).getGroup("key_value", 0);
    assertThat(entry.getString("key", 0)).isEqualTo("k");
    Group element = entry.getGroup("value", 0)
        .getGroup("list_value", 0)
        .getGroup("values", 0)
        .getGroup("list", 0)
        .getGroup("element", 0)
        .getGroup("list_value", 0)
        .getGroup("values", 0)
        .getGroup("list", 0)
        .getGroup("element", 0);
    Group truncatedEntry =
        element.getGroup("struct_value", 0).getGroup("fields", 0).getGroup("key_value", 0);
    assertThat(truncatedEntry.getString("key", 0))
        .as("keys of the truncated map stay typed and queryable")
        .isEqualTo("deep");

    Value truncated = Value.parseFrom(truncatedEntry.getBinary("value", 0).getBytes());
    assertThat(truncated.getStringValue())
        .as("truncated map value bytes are the serialized proto value")
        .isEqualTo("x");
  }

  @Test
  public void repeatedAndMapRecursionRoundTripThroughProtoReader() throws Exception {
    Trees.WideTree tree = wideTree(5, 2);
    assertThat(TestUtils.readMessages(write(tree, true, 2), Trees.WideTree.class))
        .as("specs-compliant repeated recursion reads back losslessly")
        .containsExactly(tree);
    assertThat(TestUtils.readMessages(write(tree, false, 2), Trees.WideTree.class))
        .as("old-style repeated recursion reads back losslessly")
        .containsExactly(tree);

    Struct struct = deepStruct(6);
    assertThat(TestUtils.readMessages(write(struct, true, 2), Struct.class))
        .as("map recursion reads back losslessly")
        .containsExactly(struct);
  }

  @Test
  public void optionalRecursionStillTruncatesToOptionalBytes() throws Exception {
    // regression guard for the already-working optional case (BinaryTree: left/right)
    Trees.BinaryTree.Builder tree = Trees.BinaryTree.newBuilder();
    Trees.BinaryTree.Builder cursor = tree;
    for (int i = 0; i < 6; i++) {
      cursor.getValueBuilder().setTypeUrl("level-" + i);
      cursor = cursor.getLeftBuilder();
    }
    Path file = write(tree.build(), true, 2);

    Group row = readSingleRow(file);
    Group level2 = row.getGroup("left", 0).getGroup("left", 0);
    Trees.BinaryTree truncated =
        Trees.BinaryTree.parseFrom(level2.getBinary("left", 0).getBytes());
    assertThat(truncated.getValue().getTypeUrl()).isEqualTo("level-3");
  }
}
