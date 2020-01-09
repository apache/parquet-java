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
package org.apache.parquet.schema;

import org.junit.Test;

import java.util.concurrent.Callable;

import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

public class TestTypeUtil {
  @Test
  public void testWriteCheckMessageType() {
    TypeUtil.checkValidWriteSchema(Types.buildMessage()
        .required(INT32).named("a")
        .optional(BINARY).as(UTF8).named("b")
        .named("valid_schema"));

    TestTypeBuilders.assertThrows("Should complain about empty MessageType",
        InvalidSchemaException.class,
      (Callable<Void>) () -> {
        TypeUtil.checkValidWriteSchema(new MessageType("invalid_schema"));
        return null;
      });
  }

  @Test
  public void testWriteCheckGroupType() {
    TypeUtil.checkValidWriteSchema(Types.repeatedGroup()
        .required(INT32).named("a")
        .optional(BINARY).as(UTF8).named("b")
        .named("valid_group"));

    TestTypeBuilders.assertThrows("Should complain about empty GroupType",
        InvalidSchemaException.class,
      (Callable<Void>) () -> {
        TypeUtil.checkValidWriteSchema(
            new GroupType(REPEATED, "invalid_group"));
        return null;
      });
  }

  @Test
  public void testWriteCheckNestedGroupType() {
    TypeUtil.checkValidWriteSchema(Types.buildMessage()
        .repeatedGroup()
            .required(INT32).named("a")
            .optional(BINARY).as(UTF8).named("b")
            .named("valid_group")
        .named("valid_message"));

    TestTypeBuilders.assertThrows("Should complain about empty GroupType",
        InvalidSchemaException.class,
      (Callable<Void>) () -> {
        TypeUtil.checkValidWriteSchema(Types.buildMessage()
            .addField(new GroupType(REPEATED, "invalid_group"))
            .named("invalid_message"));
        return null;
      });
  }
}
