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
package org.apache.parquet.schema;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.OriginalType.LIST;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.parquet.example.Paper;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;


public class TestMessageType {
  @Test
  public void test() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType(Paper.schema.toString());
    assertEquals(Paper.schema, schema);
    assertEquals(schema.toString(), Paper.schema.toString());
  }

  @Test
  public void testNestedTypes() {
    MessageType schema = MessageTypeParser.parseMessageType(Paper.schema.toString());
    Type type = schema.getType("Links", "Backward");
    assertEquals(PrimitiveTypeName.INT64,
        type.asPrimitiveType().getPrimitiveTypeName());
    assertEquals(0, schema.getMaxRepetitionLevel("DocId"));
    assertEquals(1, schema.getMaxRepetitionLevel("Name"));
    assertEquals(2, schema.getMaxRepetitionLevel("Name", "Language"));
    assertEquals(0, schema.getMaxDefinitionLevel("DocId"));
    assertEquals(1, schema.getMaxDefinitionLevel("Links"));
    assertEquals(2, schema.getMaxDefinitionLevel("Links", "Backward"));
  }


  @Test
  public void testMergeSchema() {
    MessageType t1 = new MessageType("root1",
        new PrimitiveType(REPEATED, BINARY, "a"),
        new PrimitiveType(OPTIONAL, BINARY, "b"));
    MessageType t2 = new MessageType("root2",
        new PrimitiveType(REQUIRED, BINARY, "c"));

    assertEquals(
        t1.union(t2),
        new MessageType("root1",
            new PrimitiveType(REPEATED, BINARY, "a"),
            new PrimitiveType(OPTIONAL, BINARY, "b"),
            new PrimitiveType(REQUIRED, BINARY, "c"))
        );

    assertEquals(
        t2.union(t1),
        new MessageType("root2",
            new PrimitiveType(REQUIRED, BINARY, "c"),
            new PrimitiveType(REPEATED, BINARY, "a"),
            new PrimitiveType(OPTIONAL, BINARY, "b"))
        );

    MessageType t3 = new MessageType("root1",
        new PrimitiveType(OPTIONAL, BINARY, "a"));
    MessageType t4 = new MessageType("root2",
        new PrimitiveType(REQUIRED, BINARY, "a"));

    assertEquals(
        t3.union(t4),
        new MessageType("root1",
            new PrimitiveType(OPTIONAL, BINARY, "a"))
        );

    assertEquals(
        t4.union(t3),
        new MessageType("root2",
            new PrimitiveType(OPTIONAL, BINARY, "a"))
        );

    MessageType t5 = new MessageType("root1",
        new GroupType(REQUIRED, "g1",
            new PrimitiveType(OPTIONAL, BINARY, "a")),
        new GroupType(REQUIRED, "g2",
            new PrimitiveType(OPTIONAL, BINARY, "b")));
    MessageType t6 = new MessageType("root1",
        new GroupType(REQUIRED, "g1",
            new PrimitiveType(OPTIONAL, BINARY, "a")),
        new GroupType(REQUIRED, "g2",
              new GroupType(REQUIRED, "g3",
                  new PrimitiveType(OPTIONAL, BINARY, "c")),
              new PrimitiveType(OPTIONAL, BINARY, "b")));

    assertEquals(
        t5.union(t6),
        new MessageType("root1",
            new GroupType(REQUIRED, "g1",
                new PrimitiveType(OPTIONAL, BINARY, "a")),
            new GroupType(REQUIRED, "g2",
                new PrimitiveType(OPTIONAL, BINARY, "b"),
                new GroupType(REQUIRED, "g3",
                    new PrimitiveType(OPTIONAL, BINARY, "c"))))
        );

    MessageType t7 = new MessageType("root1",
        new PrimitiveType(OPTIONAL, BINARY, "a"));
    MessageType t8 = new MessageType("root2",
        new PrimitiveType(OPTIONAL, INT32, "a"));
    try {
      t7.union(t8);
      fail("moving from BINARY to INT32");
    } catch (IncompatibleSchemaModificationException e) {
      assertEquals("can not merge type optional int32 a into optional binary a", e.getMessage());
    }

    MessageType t9 = Types.buildMessage()
        .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("a"))
        .named("root1");
    MessageType t10 = Types.buildMessage()
        .addField(Types.optional(BINARY).named("a"))
        .named("root1");
    assertEquals(t9.union(t9), t9);
    try {
      t9.union(t10);
      fail("moving from BINARY (UTF8) to BINARY");
    } catch (IncompatibleSchemaModificationException e) {
      assertEquals("cannot merge logical type null into STRING", e.getMessage());
    }

    MessageType t11 = Types.buildMessage()
        .addField(Types.optional(FIXED_LEN_BYTE_ARRAY).length(10).named("a"))
        .named("root1");
    MessageType t12 = Types.buildMessage()
        .addField(Types.optional(FIXED_LEN_BYTE_ARRAY).length(20).named("a"))
        .named("root2");
    try {
      t11.union(t12);
      fail("moving from FIXED_LEN_BYTE_ARRAY(10) to FIXED_LEN_BYTE_ARRAY(20)");
    } catch (IncompatibleSchemaModificationException e) {
      assertEquals("can not merge type optional fixed_len_byte_array(20) a into optional fixed_len_byte_array(10) a", e.getMessage());
    }
  }

  @Test
  public void testMergeSchemaWithOriginalType() throws Exception {
    MessageType t5 = new MessageType("root1",
        new GroupType(REQUIRED, "g1", LIST,
            new PrimitiveType(OPTIONAL, BINARY, "a")),
        new GroupType(REQUIRED, "g2",
            new PrimitiveType(OPTIONAL, BINARY, "b")));
    MessageType t6 = new MessageType("root1",
        new GroupType(REQUIRED, "g1", LIST,
            new PrimitiveType(OPTIONAL, BINARY, "a")),
        new GroupType(REQUIRED, "g2", LIST,
            new GroupType(REQUIRED, "g3",
                new PrimitiveType(OPTIONAL, BINARY, "c")),
            new PrimitiveType(OPTIONAL, BINARY, "b")));

    assertEquals(
        new MessageType("root1",
            new GroupType(REQUIRED, "g1", LIST,
                new PrimitiveType(OPTIONAL, BINARY, "a")),
            new GroupType(REQUIRED, "g2", LIST,
                new PrimitiveType(OPTIONAL, BINARY, "b"),
                new GroupType(REQUIRED, "g3",
                    new PrimitiveType(OPTIONAL, BINARY, "c")))),
        t5.union(t6));
  }

  @Test
  public void testMergeSchemaWithColumnOrder() {
    MessageType m1 = Types.buildMessage().addFields(
        Types.requiredList().element(
            Types.optional(BINARY).columnOrder(ColumnOrder.undefined()).named("a")
            ).named("g"),
        Types.optional(INT96).named("b")
        ).named("root");
    MessageType m2 = Types.buildMessage().addFields(
        Types.requiredList().element(
            Types.optional(BINARY).columnOrder(ColumnOrder.undefined()).named("a")
            ).named("g"),
        Types.optional(BINARY).named("c")
        ).named("root");
    MessageType m3 = Types.buildMessage().addFields(
        Types.requiredList().element(
            Types.optional(BINARY).named("a")
            ).named("g")
        ).named("root");

    assertEquals(
        Types.buildMessage().addFields(
            Types.requiredList().element(
                Types.optional(BINARY).named("a")
                ).named("g"),
            Types.optional(INT96).named("b"),
            Types.optional(BINARY).named("c")
            ).named("root"),
        m1.union(m2));
    try {
      m1.union(m3);
      fail("An IncompatibleSchemaModificationException should have been thrown");
    } catch (Exception e) {
      assertTrue(
          "The thrown exception should have been IncompatibleSchemaModificationException but was " + e.getClass(),
          e instanceof IncompatibleSchemaModificationException);
      assertEquals(
          "can not merge type optional binary a with column order TYPE_DEFINED_ORDER into optional binary a with column order UNDEFINED",
          e.getMessage());
    }
  }

  @Test
  public void testIDs() throws Exception {
    MessageType schema = new MessageType("test",
        new PrimitiveType(REQUIRED, BINARY, "foo").withId(4),
        new GroupType(REQUIRED, "bar",
            new PrimitiveType(REQUIRED, BINARY, "baz").withId(3)
            ).withId(8)
        );
    MessageType schema2 = MessageTypeParser.parseMessageType(schema.toString());
    assertEquals(schema, schema2);
    assertEquals(schema.toString(), schema2.toString());
  }
}
