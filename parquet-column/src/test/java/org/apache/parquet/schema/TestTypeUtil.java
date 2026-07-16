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
package org.apache.parquet.schema;

import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestTypeUtil {
  @Test
  public void testWriteCheckMessageType() {
    TypeUtil.checkValidWriteSchema(Types.buildMessage()
        .required(INT32)
        .named("a")
        .optional(BINARY)
        .as(UTF8)
        .named("b")
        .named("valid_schema"));

    MessageType invalidSchema = new MessageType("invalid_schema");
    assertThatThrownBy(() -> TypeUtil.checkValidWriteSchema(invalidSchema))
        .isInstanceOf(InvalidSchemaException.class)
        .hasMessage("Cannot write a schema with an empty group: " + invalidSchema);
  }

  @Test
  public void testWriteCheckGroupType() {
    TypeUtil.checkValidWriteSchema(Types.repeatedGroup()
        .required(INT32)
        .named("a")
        .optional(BINARY)
        .as(UTF8)
        .named("b")
        .named("valid_group"));

    GroupType invalidGroup = new GroupType(REPEATED, "invalid_group");
    assertThatThrownBy(() -> TypeUtil.checkValidWriteSchema(invalidGroup))
        .isInstanceOf(InvalidSchemaException.class)
        .hasMessage("Cannot write a schema with an empty group: " + invalidGroup);
  }

  @Test
  public void testWriteCheckNestedGroupType() {
    TypeUtil.checkValidWriteSchema(Types.buildMessage()
        .repeatedGroup()
        .required(INT32)
        .named("a")
        .optional(BINARY)
        .as(UTF8)
        .named("b")
        .named("valid_group")
        .named("valid_message"));

    MessageType invalidMessage = Types.buildMessage()
        .addField(new GroupType(REPEATED, "invalid_group"))
        .named("invalid_message");
    assertThatThrownBy(() -> TypeUtil.checkValidWriteSchema(invalidMessage))
        .isInstanceOf(InvalidSchemaException.class)
        .hasMessage("Cannot write a schema with an empty group: " + invalidMessage.getType("invalid_group"));
  }
}
