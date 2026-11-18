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

import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestTypeWithName {

  @ParameterizedTest(name = "{0}")
  @MethodSource("primitiveTypes")
  void renamingPrimitivePreservesAllOtherAttributes(
      String description, PrimitiveType original, PrimitiveType expected) {
    PrimitiveType renamed = original.withName("renamed");

    assertThat(renamed).isEqualTo(expected);
    assertThat(original.getName()).isEqualTo("original");
  }

  @SuppressWarnings("deprecation")
  private static Stream<Arguments> primitiveTypes() {
    return Stream.of(
        Arguments.of(
            "annotated fixed-width primitive with explicit column order and field ID",
            Types.optional(FIXED_LEN_BYTE_ARRAY)
                .length(4)
                .as(decimalType(2, 8))
                .columnOrder(ColumnOrder.undefined())
                .id(17)
                .named("original"),
            Types.optional(FIXED_LEN_BYTE_ARRAY)
                .length(4)
                .as(decimalType(2, 8))
                .columnOrder(ColumnOrder.undefined())
                .id(17)
                .named("renamed")),
        Arguments.of(
            "legacy decimal annotation without decimal metadata",
            new PrimitiveType(OPTIONAL, BINARY, "original", OriginalType.DECIMAL),
            new PrimitiveType(OPTIONAL, BINARY, "renamed", OriginalType.DECIMAL)),
        Arguments.of(
            "legacy decimal metadata without a logical annotation",
            new PrimitiveType(
                REPEATED,
                FIXED_LEN_BYTE_ARRAY,
                4,
                "original",
                null,
                new DecimalMetadata(8, 2),
                new Type.ID(18)),
            new PrimitiveType(
                REPEATED,
                FIXED_LEN_BYTE_ARRAY,
                4,
                "renamed",
                null,
                new DecimalMetadata(8, 2),
                new Type.ID(18))));
  }

  @Test
  void renamingGroupPreservesAnnotationFieldIdAndChildren() {
    PrimitiveType element = Types.repeated(BINARY).as(stringType()).id(22).named("element");
    GroupType original =
        Types.optionalGroup().as(listType()).id(21).addField(element).named("original");
    GroupType expected =
        Types.optionalGroup().as(listType()).id(21).addField(element).named("renamed");

    GroupType renamed = original.withName("renamed");

    assertThat(renamed).isEqualTo(expected);
    assertThat(original.getName()).isEqualTo("original");
  }

  @Test
  void renamingMessagePreservesRootSemantics() {
    MessageType original = new MessageType("original", Types.optional(INT32).named("value"));
    MessageType expected = new MessageType("renamed", Types.optional(INT32).named("value"));

    MessageType renamed = original.withName("renamed");

    assertThat(renamed).isEqualTo(expected);
    assertThat(renamed.getMaxDefinitionLevel("value")).isEqualTo(1);
    assertThat(renamed.getMaxRepetitionLevel("value")).isZero();
    assertThat(original.getName()).isEqualTo("original");
  }

  @Test
  void renamingRejectsNullName() {
    Type type = Types.optional(INT32).named("original");

    assertThatThrownBy(() -> type.withName(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("name cannot be null");
  }
}
