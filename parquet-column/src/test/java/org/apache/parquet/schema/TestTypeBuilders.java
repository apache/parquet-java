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

import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.OriginalType.BSON;
import static org.apache.parquet.schema.OriginalType.DATE;
import static org.apache.parquet.schema.OriginalType.DECIMAL;
import static org.apache.parquet.schema.OriginalType.INTERVAL;
import static org.apache.parquet.schema.OriginalType.INT_16;
import static org.apache.parquet.schema.OriginalType.INT_32;
import static org.apache.parquet.schema.OriginalType.INT_64;
import static org.apache.parquet.schema.OriginalType.INT_8;
import static org.apache.parquet.schema.OriginalType.JSON;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MICROS;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.OriginalType.TIME_MICROS;
import static org.apache.parquet.schema.OriginalType.TIME_MILLIS;
import static org.apache.parquet.schema.OriginalType.UINT_16;
import static org.apache.parquet.schema.OriginalType.UINT_32;
import static org.apache.parquet.schema.OriginalType.UINT_64;
import static org.apache.parquet.schema.OriginalType.UINT_8;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.jupiter.api.Test;

public class TestTypeBuilders {
  @Test
  public void testPaperExample() {
    MessageType expected = new MessageType(
        "Document",
        new PrimitiveType(REQUIRED, INT64, "DocId"),
        new GroupType(
            OPTIONAL,
            "Links",
            new PrimitiveType(REPEATED, INT64, "Backward"),
            new PrimitiveType(REPEATED, INT64, "Forward")),
        new GroupType(
            REPEATED,
            "Name",
            new GroupType(
                REPEATED,
                "Language",
                new PrimitiveType(REQUIRED, BINARY, "Code"),
                new PrimitiveType(REQUIRED, BINARY, "Country")),
            new PrimitiveType(OPTIONAL, BINARY, "Url")));
    MessageType builderType = Types.buildMessage()
        .required(INT64)
        .named("DocId")
        .optionalGroup()
        .repeated(INT64)
        .named("Backward")
        .repeated(INT64)
        .named("Forward")
        .named("Links")
        .repeatedGroup()
        .repeatedGroup()
        .required(BINARY)
        .named("Code")
        .required(BINARY)
        .named("Country")
        .named("Language")
        .optional(BINARY)
        .named("Url")
        .named("Name")
        .named("Document");
    assertThat(builderType).isEqualTo(expected);
  }

  @Test
  public void testGroupTypeConstruction() {
    PrimitiveType f1 = Types.required(BINARY).as(UTF8).named("f1");
    PrimitiveType f2 = Types.required(INT32).named("f2");
    PrimitiveType f3 = Types.optional(INT32).named("f3");
    String name = "group";
    for (Type.Repetition repetition : Type.Repetition.values()) {
      GroupType expected = new GroupType(repetition, name, f1, new GroupType(repetition, "g1", f2, f3));
      GroupType built = Types.buildGroup(repetition)
          .addField(f1)
          .group(repetition)
          .addFields(f2, f3)
          .named("g1")
          .named(name);
      assertThat(built).isEqualTo(expected);

      switch (repetition) {
        case REQUIRED:
          built = Types.requiredGroup()
              .addField(f1)
              .requiredGroup()
              .addFields(f2, f3)
              .named("g1")
              .named(name);
          break;
        case OPTIONAL:
          built = Types.optionalGroup()
              .addField(f1)
              .optionalGroup()
              .addFields(f2, f3)
              .named("g1")
              .named(name);
          break;
        case REPEATED:
          built = Types.repeatedGroup()
              .addField(f1)
              .repeatedGroup()
              .addFields(f2, f3)
              .named("g1")
              .named(name);
          break;
      }
      assertThat(built).isEqualTo(expected);
    }
  }

  @Test
  public void testPrimitiveTypeConstruction() {
    PrimitiveTypeName[] types = new PrimitiveTypeName[] {BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BINARY};
    for (PrimitiveTypeName type : types) {
      String name = type + "_";
      for (Type.Repetition repetition : Type.Repetition.values()) {
        PrimitiveType expected = new PrimitiveType(repetition, type, name);
        PrimitiveType built = Types.primitive(type, repetition).named(name);
        assertThat(built).isEqualTo(expected);
        switch (repetition) {
          case REQUIRED:
            built = Types.required(type).named(name);
            break;
          case OPTIONAL:
            built = Types.optional(type).named(name);
            break;
          case REPEATED:
            built = Types.repeated(type).named(name);
            break;
        }
        assertThat(built).isEqualTo(expected);
      }
    }
  }

  @Test
  public void testFixedTypeConstruction() {
    String name = "fixed_";
    int len = 5;
    for (Type.Repetition repetition : Type.Repetition.values()) {
      PrimitiveType expected = new PrimitiveType(repetition, FIXED_LEN_BYTE_ARRAY, len, name);
      PrimitiveType built = Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .length(len)
          .named(name);
      assertThat(built).isEqualTo(expected);
      switch (repetition) {
        case REQUIRED:
          built = Types.required(FIXED_LEN_BYTE_ARRAY).length(len).named(name);
          break;
        case OPTIONAL:
          built = Types.optional(FIXED_LEN_BYTE_ARRAY).length(len).named(name);
          break;
        case REPEATED:
          built = Types.repeated(FIXED_LEN_BYTE_ARRAY).length(len).named(name);
          break;
      }
      assertThat(built).isEqualTo(expected);
    }
  }

  @Test
  public void testEmptyGroup() {
    // empty groups are allowed to support selecting 0 columns (counting rows)
    assertThat(new GroupType(REQUIRED, "g"))
        .as("Should not complain about an empty required group")
        .isEqualTo(Types.requiredGroup().named("g"));
    assertThat(new GroupType(OPTIONAL, "g"))
        .as("Should not complain about an empty required group")
        .isEqualTo(Types.optionalGroup().named("g"));
    assertThat(new GroupType(REPEATED, "g"))
        .as("Should not complain about an empty required group")
        .isEqualTo(Types.repeatedGroup().named("g"));
  }

  @Test
  public void testEmptyMessage() {
    // empty groups are allowed to support selecting 0 columns (counting rows)
    assertThat(new MessageType("m"))
        .as("Should not complain about an empty required group")
        .isEqualTo(Types.buildMessage().named("m"));
  }

  @Test
  public void testFixedWithoutLength() {
    assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY).named("fixed"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid FIXED_LEN_BYTE_ARRAY length: 0");
  }

  @Test
  public void testFixedWithLength() {
    PrimitiveType expected = new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, 7, "fixed");
    PrimitiveType fixed = Types.required(FIXED_LEN_BYTE_ARRAY).length(7).named("fixed");
    assertThat(fixed).isEqualTo(expected);
  }

  @Test
  public void testFixedLengthEquals() {
    Type f4 = Types.required(FIXED_LEN_BYTE_ARRAY).length(4).named("f4");
    Type f8 = Types.required(FIXED_LEN_BYTE_ARRAY).length(8).named("f8");
    assertThat(f4).as("Types with different lengths should not be equal").isNotEqualTo(f8);
  }

  @Test
  public void testDecimalAnnotation() {
    // int32 primitive type
    MessageType expected = new MessageType(
        "DecimalMessage",
        new PrimitiveType(REQUIRED, INT32, 0, "aDecimal", DECIMAL, new DecimalMetadata(9, 2), null));
    MessageType builderType = Types.buildMessage()
        .required(INT32)
        .as(DECIMAL)
        .precision(9)
        .scale(2)
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);
    // int64 primitive type
    expected = new MessageType(
        "DecimalMessage",
        new PrimitiveType(REQUIRED, INT64, 0, "aDecimal", DECIMAL, new DecimalMetadata(18, 2), null));
    builderType = Types.buildMessage()
        .required(INT64)
        .as(DECIMAL)
        .precision(18)
        .scale(2)
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);
    // binary primitive type
    expected = new MessageType(
        "DecimalMessage",
        new PrimitiveType(REQUIRED, BINARY, 0, "aDecimal", DECIMAL, new DecimalMetadata(9, 2), null));
    builderType = Types.buildMessage()
        .required(BINARY)
        .as(DECIMAL)
        .precision(9)
        .scale(2)
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);
    // fixed primitive type
    expected = new MessageType(
        "DecimalMessage",
        new PrimitiveType(
            REQUIRED, FIXED_LEN_BYTE_ARRAY, 4, "aDecimal", DECIMAL, new DecimalMetadata(9, 2), null));
    builderType = Types.buildMessage()
        .required(FIXED_LEN_BYTE_ARRAY)
        .length(4)
        .as(DECIMAL)
        .precision(9)
        .scale(2)
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);
  }

  @Test
  public void testDecimalAnnotationMissingScale() {
    MessageType expected = new MessageType(
        "DecimalMessage",
        new PrimitiveType(REQUIRED, INT32, 0, "aDecimal", DECIMAL, new DecimalMetadata(9, 0), null));
    MessageType builderType = Types.buildMessage()
        .required(INT32)
        .as(DECIMAL)
        .precision(9)
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);

    expected = new MessageType(
        "DecimalMessage",
        new PrimitiveType(REQUIRED, INT64, 0, "aDecimal", DECIMAL, new DecimalMetadata(9, 0), null));
    builderType = Types.buildMessage()
        .required(INT64)
        .as(DECIMAL)
        .precision(9)
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);

    expected = new MessageType(
        "DecimalMessage",
        new PrimitiveType(REQUIRED, BINARY, 0, "aDecimal", DECIMAL, new DecimalMetadata(9, 0), null));
    builderType = Types.buildMessage()
        .required(BINARY)
        .as(DECIMAL)
        .precision(9)
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);

    expected = new MessageType(
        "DecimalMessage",
        new PrimitiveType(
            REQUIRED, FIXED_LEN_BYTE_ARRAY, 7, "aDecimal", DECIMAL, new DecimalMetadata(9, 0), null));
    builderType = Types.buildMessage()
        .required(FIXED_LEN_BYTE_ARRAY)
        .length(7)
        .as(DECIMAL)
        .precision(9)
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);
  }

  @Test
  public void testDecimalAnnotationMissingPrecision() {
    assertThatThrownBy(() -> Types.buildMessage()
            .required(INT32)
            .as(DECIMAL)
            .scale(2)
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DECIMAL precision: 0");
    assertThatThrownBy(() -> Types.buildMessage()
            .required(INT64)
            .as(DECIMAL)
            .scale(2)
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DECIMAL precision: 0");
    assertThatThrownBy(() -> Types.buildMessage()
            .required(BINARY)
            .as(DECIMAL)
            .scale(2)
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DECIMAL precision: 0");
    assertThatThrownBy(() -> Types.buildMessage()
            .required(FIXED_LEN_BYTE_ARRAY)
            .length(7)
            .as(DECIMAL)
            .scale(2)
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DECIMAL precision: 0");
  }

  @Test
  public void testDecimalAnnotationPrecisionScaleBound() {
    assertThatThrownBy(() -> Types.buildMessage()
            .required(INT32)
            .as(DECIMAL)
            .precision(3)
            .scale(4)
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DECIMAL scale: 4 cannot be greater than precision: 3");
    assertThatThrownBy(() -> Types.buildMessage()
            .required(INT64)
            .as(DECIMAL)
            .precision(3)
            .scale(4)
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DECIMAL scale: 4 cannot be greater than precision: 3");
    assertThatThrownBy(() -> Types.buildMessage()
            .required(BINARY)
            .as(DECIMAL)
            .precision(3)
            .scale(4)
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DECIMAL scale: 4 cannot be greater than precision: 3");
    assertThatThrownBy(() -> Types.buildMessage()
            .required(FIXED_LEN_BYTE_ARRAY)
            .length(7)
            .as(DECIMAL)
            .precision(3)
            .scale(4)
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid DECIMAL scale: 4 cannot be greater than precision: 3");
  }

  @Test
  public void testDecimalAnnotationLengthCheck() {
    // maximum precision for 4 bytes is 9
    assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(4)
            .as(DECIMAL)
            .precision(10)
            .scale(2)
            .named("aDecimal"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("FIXED(4) cannot store 10 digits (max 9)");
    assertThatThrownBy(() ->
            Types.required(INT32).as(DECIMAL).precision(10).scale(2).named("aDecimal"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("INT32 cannot store 10 digits (max 9)");
    // maximum precision for 8 bytes is 19
    assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(8)
            .as(DECIMAL)
            .precision(19)
            .scale(4)
            .named("aDecimal"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("FIXED(8) cannot store 19 digits (max 18)");
    assertThatThrownBy(() -> Types.required(INT64)
            .length(8)
            .as(DECIMAL)
            .precision(19)
            .scale(4)
            .named("aDecimal"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("INT64 cannot store 19 digits (max 18)");
  }

  @Test
  public void testDECIMALAnnotationRejectsUnsupportedTypes() {
    PrimitiveTypeName[] unsupported = new PrimitiveTypeName[] {BOOLEAN, INT96, DOUBLE, FLOAT};
    for (final PrimitiveTypeName type : unsupported) {
      assertThatThrownBy(() -> Types.required(type)
              .as(DECIMAL)
              .precision(9)
              .scale(2)
              .named("d"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("DECIMAL can only annotate INT32, INT64, BINARY, and FIXED");
    }
  }

  @Test
  public void testBinaryAnnotations() {
    OriginalType[] types = new OriginalType[] {UTF8, JSON, BSON};
    for (final OriginalType logicalType : types) {
      PrimitiveType expected = new PrimitiveType(REQUIRED, BINARY, "col", logicalType);
      PrimitiveType string = Types.required(BINARY).as(logicalType).named("col");
      assertThat(string).isEqualTo(expected);
    }
  }

  @Test
  public void testBinaryAnnotationsRejectsNonBinary() {
    OriginalType[] types = new OriginalType[] {UTF8, JSON, BSON};
    for (final OriginalType logicalType : types) {
      PrimitiveTypeName[] nonBinary = new PrimitiveTypeName[] {BOOLEAN, INT32, INT64, INT96, DOUBLE, FLOAT};
      for (final PrimitiveTypeName type : nonBinary) {
        assertThatThrownBy(() -> Types.required(type).as(logicalType).named("col"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage(LogicalTypeAnnotation.fromOriginalType(logicalType, null)
                + " can only annotate BINARY");
      }
      assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
              .length(1)
              .as(logicalType)
              .named("col"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(
              LogicalTypeAnnotation.fromOriginalType(logicalType, null) + " can only annotate BINARY");
    }
  }

  @Test
  public void testInt32Annotations() {
    OriginalType[] types = new OriginalType[] {DATE, TIME_MILLIS, UINT_8, UINT_16, UINT_32, INT_8, INT_16, INT_32};
    for (OriginalType logicalType : types) {
      PrimitiveType expected = new PrimitiveType(REQUIRED, INT32, "col", logicalType);
      PrimitiveType date = Types.required(INT32).as(logicalType).named("col");
      assertThat(date).isEqualTo(expected);
    }
  }

  @Test
  public void testInt32AnnotationsRejectNonInt32() {
    OriginalType[] types = new OriginalType[] {DATE, TIME_MILLIS, UINT_8, UINT_16, UINT_32, INT_8, INT_16, INT_32};
    for (final OriginalType logicalType : types) {
      PrimitiveTypeName[] nonInt32 = new PrimitiveTypeName[] {BOOLEAN, INT64, INT96, DOUBLE, FLOAT, BINARY};
      for (final PrimitiveTypeName type : nonInt32) {
        assertThatThrownBy(() -> Types.required(type).as(logicalType).named("col"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage(
                LogicalTypeAnnotation.fromOriginalType(logicalType, null) + " can only annotate INT32");
      }
      assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
              .length(1)
              .as(logicalType)
              .named("col"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(LogicalTypeAnnotation.fromOriginalType(logicalType, null) + " can only annotate INT32");
    }
  }

  @Test
  public void testInt64Annotations() {
    OriginalType[] types = new OriginalType[] {TIME_MICROS, TIMESTAMP_MILLIS, TIMESTAMP_MICROS, UINT_64, INT_64};
    for (OriginalType logicalType : types) {
      PrimitiveType expected = new PrimitiveType(REQUIRED, INT64, "col", logicalType);
      PrimitiveType date = Types.required(INT64).as(logicalType).named("col");
      assertThat(date).isEqualTo(expected);
    }
  }

  @Test
  public void testInt64AnnotationsRejectNonInt64() {
    OriginalType[] types = new OriginalType[] {TIME_MICROS, TIMESTAMP_MILLIS, TIMESTAMP_MICROS, UINT_64, INT_64};
    for (final OriginalType logicalType : types) {
      PrimitiveTypeName[] nonInt64 = new PrimitiveTypeName[] {BOOLEAN, INT32, INT96, DOUBLE, FLOAT, BINARY};
      for (final PrimitiveTypeName type : nonInt64) {
        assertThatThrownBy(() -> Types.required(type).as(logicalType).named("col"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage(
                LogicalTypeAnnotation.fromOriginalType(logicalType, null) + " can only annotate INT64");
      }
      // TIMESTAMP allows FLBA(12); other lengths are still rejected.
      // Non-timestamp types still only accept INT64 for any FLBA length.
      boolean isTimestamp = logicalType == TIMESTAMP_MILLIS || logicalType == TIMESTAMP_MICROS;
      String flbaErrMsg = isTimestamp
          ? LogicalTypeAnnotation.fromOriginalType(logicalType, null)
              + " can only annotate FIXED_LEN_BYTE_ARRAY(12)"
          : LogicalTypeAnnotation.fromOriginalType(logicalType, null) + " can only annotate INT64";
      assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
              .length(1)
              .as(logicalType)
              .named("col"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(flbaErrMsg);
    }
  }

  @Test
  public void testIntervalAnnotation() {
    PrimitiveType expected = new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, 12, "interval", INTERVAL);
    PrimitiveType string =
        Types.required(FIXED_LEN_BYTE_ARRAY).length(12).as(INTERVAL).named("interval");
    assertThat(string).isEqualTo(expected);
  }

  @Test
  public void testIntervalAnnotationRejectsNonFixed() {
    PrimitiveTypeName[] nonFixed = new PrimitiveTypeName[] {BOOLEAN, INT32, INT64, INT96, DOUBLE, FLOAT, BINARY};
    for (final PrimitiveTypeName type : nonFixed) {
      assertThatThrownBy(() -> Types.required(type).as(INTERVAL).named("interval"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)");
    }
  }

  @Test
  public void testIntervalAnnotationRejectsNonFixed12() {
    assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(11)
            .as(INTERVAL)
            .named("interval"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)");
  }

  @Test
  public void testRequiredMap() {
    List<Type> typeList = new ArrayList<>();
    typeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    typeList.add(new PrimitiveType(REQUIRED, INT64, "value"));
    GroupType expected =
        new GroupType(REQUIRED, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));
    GroupType actual = Types.requiredMap().key(INT64).requiredValue(INT64).named("myMap");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testOptionalMap() {
    List<Type> typeList = new ArrayList<>();
    typeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    typeList.add(new PrimitiveType(REQUIRED, INT64, "value"));
    GroupType expected =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));
    GroupType actual = Types.optionalMap().key(INT64).requiredValue(INT64).named("myMap");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithRequiredValue() {
    List<Type> typeList = new ArrayList<>();
    typeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    typeList.add(new PrimitiveType(REQUIRED, INT64, "value"));
    GroupType map =
        new GroupType(REQUIRED, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));
    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .requiredMap()
        .key(INT64)
        .requiredValue(INT64)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithOptionalValue() {
    List<Type> typeList = new ArrayList<>();
    typeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    typeList.add(new PrimitiveType(OPTIONAL, INT64, "value"));
    GroupType map =
        new GroupType(REQUIRED, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));
    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .requiredMap()
        .key(INT64)
        .optionalValue(INT64)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithGroupKeyAndOptionalGroupValue() {
    List<Type> typeList = new ArrayList<>();

    List<Type> keyFields = new ArrayList<>();
    keyFields.add(new PrimitiveType(OPTIONAL, INT64, "first"));
    keyFields.add(new PrimitiveType(OPTIONAL, DOUBLE, "second"));
    typeList.add(new GroupType(REQUIRED, "key", keyFields));

    List<Type> valueFields = new ArrayList<>();
    valueFields.add(new PrimitiveType(OPTIONAL, DOUBLE, "one"));
    valueFields.add(new PrimitiveType(OPTIONAL, INT32, "two"));
    typeList.add(new GroupType(OPTIONAL, "value", valueFields));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    GroupType actual = Types.optionalMap()
        .groupKey()
        .optional(INT64)
        .named("first")
        .optional(DOUBLE)
        .named("second")
        .optionalGroupValue()
        .optional(DOUBLE)
        .named("one")
        .optional(INT32)
        .named("two")
        .named("myMap");
    assertThat(actual).isEqualTo(map);
  }

  @Test
  public void testMapWithGroupKeyAndRequiredGroupValue() {
    List<Type> typeList = new ArrayList<>();

    List<Type> keyFields = new ArrayList<>();
    keyFields.add(new PrimitiveType(OPTIONAL, INT64, "first"));
    keyFields.add(new PrimitiveType(OPTIONAL, DOUBLE, "second"));
    typeList.add(new GroupType(REQUIRED, "key", keyFields));

    List<Type> valueFields = new ArrayList<>();
    valueFields.add(new PrimitiveType(OPTIONAL, DOUBLE, "one"));
    valueFields.add(new PrimitiveType(OPTIONAL, INT32, "two"));
    typeList.add(new GroupType(REQUIRED, "value", valueFields));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .groupKey()
        .optional(INT64)
        .named("first")
        .optional(DOUBLE)
        .named("second")
        .requiredGroupValue()
        .optional(DOUBLE)
        .named("one")
        .optional(INT32)
        .named("two")
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithGroupKeyAndOptionalValue() {
    List<Type> typeList = new ArrayList<>();

    List<Type> keyFields = new ArrayList<>();
    keyFields.add(new PrimitiveType(OPTIONAL, INT64, "first"));
    keyFields.add(new PrimitiveType(OPTIONAL, DOUBLE, "second"));
    typeList.add(new GroupType(REQUIRED, "key", keyFields));

    typeList.add(new PrimitiveType(OPTIONAL, DOUBLE, "value"));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .groupKey()
        .optional(INT64)
        .named("first")
        .optional(DOUBLE)
        .named("second")
        .optionalValue(DOUBLE)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithGroupKeyAndRequiredValue() {
    List<Type> typeList = new ArrayList<>();

    List<Type> keyFields = new ArrayList<>();
    keyFields.add(new PrimitiveType(OPTIONAL, INT64, "first"));
    keyFields.add(new PrimitiveType(OPTIONAL, DOUBLE, "second"));
    typeList.add(new GroupType(REQUIRED, "key", keyFields));

    typeList.add(new PrimitiveType(REQUIRED, DOUBLE, "value"));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .groupKey()
        .optional(INT64)
        .named("first")
        .optional(DOUBLE)
        .named("second")
        .requiredValue(DOUBLE)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithOptionalGroupValue() {
    List<Type> typeList = new ArrayList<>();

    List<Type> keyFields = new ArrayList<>();
    keyFields.add(new PrimitiveType(OPTIONAL, INT64, "first"));
    keyFields.add(new PrimitiveType(OPTIONAL, DOUBLE, "second"));
    typeList.add(new PrimitiveType(REQUIRED, INT64, "key"));

    List<Type> valueFields = new ArrayList<>();
    valueFields.add(new PrimitiveType(OPTIONAL, DOUBLE, "one"));
    valueFields.add(new PrimitiveType(OPTIONAL, INT32, "two"));
    typeList.add(new GroupType(OPTIONAL, "value", valueFields));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .key(INT64)
        .optionalGroupValue()
        .optional(DOUBLE)
        .named("one")
        .optional(INT32)
        .named("two")
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithRequiredGroupValue() {
    List<Type> typeList = new ArrayList<>();

    typeList.add(new PrimitiveType(REQUIRED, INT64, "key"));

    List<Type> valueFields = new ArrayList<>();
    valueFields.add(new PrimitiveType(OPTIONAL, DOUBLE, "one"));
    valueFields.add(new PrimitiveType(OPTIONAL, INT32, "two"));
    typeList.add(new GroupType(REQUIRED, "value", valueFields));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .key(INT64)
        .requiredGroupValue()
        .optional(DOUBLE)
        .named("one")
        .optional(INT32)
        .named("two")
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithNestedGroupKeyAndNestedGroupValue() {
    List<Type> typeList = new ArrayList<>();

    List<Type> innerFields = new ArrayList<>();
    innerFields.add(new PrimitiveType(REQUIRED, FLOAT, "inner_key_1"));
    innerFields.add(new PrimitiveType(OPTIONAL, INT32, "inner_key_2"));

    List<Type> keyFields = new ArrayList<>();
    keyFields.add(new PrimitiveType(OPTIONAL, INT64, "first"));
    keyFields.add(new GroupType(REQUIRED, "second", innerFields));
    typeList.add(new GroupType(REQUIRED, "key", keyFields));

    List<Type> valueFields = new ArrayList<>();
    valueFields.add(new GroupType(OPTIONAL, "one", innerFields));
    valueFields.add(new PrimitiveType(OPTIONAL, INT32, "two"));
    typeList.add(new GroupType(OPTIONAL, "value", valueFields));

    GroupType map =
        new GroupType(REQUIRED, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .requiredMap()
        .groupKey()
        .optional(INT64)
        .named("first")
        .requiredGroup()
        .required(FLOAT)
        .named("inner_key_1")
        .optional(INT32)
        .named("inner_key_2")
        .named("second")
        .optionalGroupValue()
        .optionalGroup()
        .required(FLOAT)
        .named("inner_key_1")
        .optional(INT32)
        .named("inner_key_2")
        .named("one")
        .optional(INT32)
        .named("two")
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithRequiredListValue() {
    List<Type> typeList = new ArrayList<>();

    typeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    typeList.add(new GroupType(
        REQUIRED,
        "value",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(OPTIONAL, INT64, "element"))));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .key(INT64)
        .requiredListValue()
        .optionalElement(INT64)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithOptionalListValue() {
    List<Type> typeList = new ArrayList<>();

    typeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    typeList.add(new GroupType(
        OPTIONAL,
        "value",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(OPTIONAL, INT64, "element"))));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .key(INT64)
        .optionalListValue()
        .optionalElement(INT64)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithRequiredMapValue() {
    List<Type> typeList = new ArrayList<>();

    List<Type> innerMapTypeList = new ArrayList<>();
    innerMapTypeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    innerMapTypeList.add(new PrimitiveType(REQUIRED, INT64, "value"));

    typeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    typeList.add(new GroupType(
        REQUIRED, "value", OriginalType.MAP, new GroupType(REPEATED, "key_value", innerMapTypeList)));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .key(INT64)
        .requiredMapValue()
        .key(INT64)
        .requiredValue(INT64)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithOptionalMapValue() {
    List<Type> typeList = new ArrayList<>();

    List<Type> innerMapTypeList = new ArrayList<>();
    innerMapTypeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    innerMapTypeList.add(new PrimitiveType(REQUIRED, INT64, "value"));

    typeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    typeList.add(new GroupType(
        OPTIONAL, "value", OriginalType.MAP, new GroupType(REPEATED, "key_value", innerMapTypeList)));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .key(INT64)
        .optionalMapValue()
        .key(INT64)
        .requiredValue(INT64)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithGroupKeyAndRequiredListValue() {
    List<Type> typeList = new ArrayList<>();

    typeList.add(new GroupType(REQUIRED, "key", new PrimitiveType(REQUIRED, INT64, "first")));
    typeList.add(new GroupType(
        REQUIRED,
        "value",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(OPTIONAL, INT64, "element"))));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .groupKey()
        .required(INT64)
        .named("first")
        .requiredListValue()
        .optionalElement(INT64)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithGroupKeyAndOptionalListValue() {
    List<Type> typeList = new ArrayList<>();

    typeList.add(new GroupType(REQUIRED, "key", new PrimitiveType(REQUIRED, INT64, "first")));
    typeList.add(new GroupType(
        OPTIONAL,
        "value",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(OPTIONAL, INT64, "element"))));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .groupKey()
        .required(INT64)
        .named("first")
        .optionalListValue()
        .optionalElement(INT64)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithGroupKeyAndRequiredMapValue() {
    List<Type> typeList = new ArrayList<>();

    List<Type> innerMapTypeList = new ArrayList<>();
    innerMapTypeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    innerMapTypeList.add(new PrimitiveType(REQUIRED, INT64, "value"));

    typeList.add(new GroupType(REQUIRED, "key", new PrimitiveType(REQUIRED, INT64, "first")));
    typeList.add(new GroupType(
        REQUIRED, "value", OriginalType.MAP, new GroupType(REPEATED, "key_value", innerMapTypeList)));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .groupKey()
        .required(INT64)
        .named("first")
        .requiredMapValue()
        .key(INT64)
        .requiredValue(INT64)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithGroupKeyAndOptionalMapValue() {
    List<Type> typeList = new ArrayList<>();

    List<Type> innerMapTypeList = new ArrayList<>();
    innerMapTypeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    innerMapTypeList.add(new PrimitiveType(REQUIRED, INT64, "value"));

    typeList.add(new GroupType(REQUIRED, "key", new PrimitiveType(REQUIRED, INT64, "first")));
    typeList.add(new GroupType(
        OPTIONAL, "value", OriginalType.MAP, new GroupType(REPEATED, "key_value", innerMapTypeList)));

    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage()
        .optionalMap()
        .groupKey()
        .required(INT64)
        .named("first")
        .optionalMapValue()
        .key(INT64)
        .requiredValue(INT64)
        .named("myMap")
        .named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithNullValue() {
    List<Type> typeList = new ArrayList<>();

    typeList.add(new PrimitiveType(REQUIRED, INT64, "key"));
    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual =
        Types.buildMessage().optionalMap().key(INT64).named("myMap").named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithDefaultKeyAndNullValue() {
    List<Type> typeList = new ArrayList<>();

    typeList.add(new PrimitiveType(REQUIRED, BINARY, "key", OriginalType.UTF8));
    GroupType map =
        new GroupType(OPTIONAL, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", typeList));

    MessageType expected = new MessageType("mapParent", map);
    GroupType actual = Types.buildMessage().optionalMap().named("myMap").named("mapParent");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapWithPreBuiltKeyAndValueTypes() {
    Type keyType = Types.required(INT64).named("key");
    Type valueType = Types.required(BOOLEAN).named("value");

    GroupType map = new GroupType(
        REQUIRED, "myMap", OriginalType.MAP, new GroupType(REPEATED, "key_value", new Type[] {keyType, valueType
        }));
    MessageType expected = new MessageType("mapParent", map);

    GroupType actual = Types.buildMessage()
        .requiredMap()
        .key(keyType)
        .value(valueType)
        .named("myMap")
        .named("mapParent");

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testListWithRequiredPreBuiltElement() {
    GroupType expected = new GroupType(
        REQUIRED,
        "myList",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(REQUIRED, INT64, "element")));
    Type element = Types.primitive(INT64, REQUIRED).named("element");
    Type actual = Types.requiredList().element(element).named("myList");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testRequiredList() {
    GroupType expected = new GroupType(
        REQUIRED,
        "myList",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(OPTIONAL, INT64, "element")));
    Type actual = Types.requiredList().optionalElement(INT64).named("myList");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testOptionalList() {
    GroupType expected = new GroupType(
        OPTIONAL,
        "myList",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(OPTIONAL, INT64, "element")));
    Type actual = Types.optionalList().optionalElement(INT64).named("myList");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testListOfReqGroup() {
    List<Type> fields = new ArrayList<>();
    fields.add(new PrimitiveType(OPTIONAL, BOOLEAN, "field"));
    GroupType expected = new GroupType(
        REQUIRED,
        "myList",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new GroupType(REQUIRED, "element", fields)));
    Type actual = Types.requiredList()
        .requiredGroupElement()
        .optional(BOOLEAN)
        .named("field")
        .named("myList");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testListOfOptionalGroup() {
    List<Type> fields = new ArrayList<>();
    fields.add(new PrimitiveType(OPTIONAL, BOOLEAN, "field"));
    GroupType expected = new GroupType(
        REQUIRED,
        "myList",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new GroupType(OPTIONAL, "element", fields)));
    Type actual = Types.requiredList()
        .optionalGroupElement()
        .optional(BOOLEAN)
        .named("field")
        .named("myList");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testRequiredNestedList() {
    List<Type> fields = new ArrayList<>();
    fields.add(new GroupType(
        REQUIRED,
        "element",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(OPTIONAL, DOUBLE, "element"))));
    GroupType expected =
        new GroupType(OPTIONAL, "myList", OriginalType.LIST, new GroupType(REPEATED, "list", fields));

    Type actual = Types.optionalList()
        .requiredListElement()
        .optionalElement(DOUBLE)
        .named("myList");

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testOptionalNestedList() {
    List<Type> fields = new ArrayList<>();
    fields.add(new GroupType(
        OPTIONAL,
        "element",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(OPTIONAL, DOUBLE, "element"))));
    GroupType expected =
        new GroupType(OPTIONAL, "myList", OriginalType.LIST, new GroupType(REPEATED, "list", fields));

    Type actual = Types.optionalList()
        .optionalListElement()
        .optionalElement(DOUBLE)
        .named("myList");

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testRequiredListWithinGroup() {
    List<Type> fields = new ArrayList<>();
    fields.add(new GroupType(
        REQUIRED,
        "element",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(OPTIONAL, INT64, "element"))));
    GroupType expected = new GroupType(REQUIRED, "topGroup", fields);
    Type actual = Types.requiredGroup()
        .requiredList()
        .optionalElement(INT64)
        .named("element")
        .named("topGroup");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testOptionalListWithinGroup() {
    List<Type> fields = new ArrayList<>();
    fields.add(new GroupType(
        OPTIONAL,
        "element",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(OPTIONAL, INT64, "element"))));
    GroupType expected = new GroupType(REQUIRED, "topGroup", fields);
    Type actual = Types.requiredGroup()
        .optionalList()
        .optionalElement(INT64)
        .named("element")
        .named("topGroup");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testOptionalListWithinGroupWithReqElement() {
    List<Type> fields = new ArrayList<>();
    fields.add(new GroupType(
        OPTIONAL,
        "element",
        OriginalType.LIST,
        new GroupType(REPEATED, "list", new PrimitiveType(REQUIRED, INT64, "element"))));
    GroupType expected = new GroupType(REQUIRED, "topGroup", fields);
    Type actual = Types.requiredGroup()
        .optionalList()
        .requiredElement(INT64)
        .named("element")
        .named("topGroup");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testRequiredMapWithinList() {
    List<Type> innerFields = new ArrayList<>();
    innerFields.add(new PrimitiveType(REQUIRED, DOUBLE, "key"));
    innerFields.add(new PrimitiveType(REQUIRED, INT32, "value"));

    List<Type> fields = new ArrayList<>();
    fields.add(new GroupType(
        REQUIRED, "element", OriginalType.MAP, new GroupType(REPEATED, "key_value", innerFields)));
    GroupType expected =
        new GroupType(OPTIONAL, "myList", OriginalType.LIST, new GroupType(REPEATED, "list", fields));

    Type actual = Types.optionalList()
        .requiredMapElement()
        .key(DOUBLE)
        .requiredValue(INT32)
        .named("myList");

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testOptionalMapWithinList() {
    List<Type> innerFields = new ArrayList<>();
    innerFields.add(new PrimitiveType(REQUIRED, DOUBLE, "key"));
    innerFields.add(new PrimitiveType(REQUIRED, INT32, "value"));

    List<Type> fields = new ArrayList<>();
    fields.add(new GroupType(
        OPTIONAL, "element", OriginalType.MAP, new GroupType(REPEATED, "key_value", innerFields)));
    GroupType expected =
        new GroupType(OPTIONAL, "myList", OriginalType.LIST, new GroupType(REPEATED, "list", fields));

    Type actual = Types.optionalList()
        .optionalMapElement()
        .key(DOUBLE)
        .requiredValue(INT32)
        .named("myList");

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testTypeConstructionWithUndefinedColumnOrder() {
    PrimitiveTypeName[] types =
        new PrimitiveTypeName[] {BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BINARY, FIXED_LEN_BYTE_ARRAY};
    for (PrimitiveTypeName type : types) {
      String name = type.toString() + "_";
      int len = type == FIXED_LEN_BYTE_ARRAY ? 42 : 0;
      PrimitiveType expected =
          new PrimitiveType(Repetition.OPTIONAL, type, len, name, null, null, null, ColumnOrder.undefined());
      PrimitiveType built = Types.optional(type)
          .length(len)
          .columnOrder(ColumnOrder.undefined())
          .named(name);
      assertThat(built).isEqualTo(expected);
    }
  }

  @Test
  public void testTypeConstructionWithTypeDefinedColumnOrder() {
    PrimitiveTypeName[] types =
        new PrimitiveTypeName[] {BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BINARY, FIXED_LEN_BYTE_ARRAY};
    for (PrimitiveTypeName type : types) {
      String name = type.toString() + "_";
      int len = type == FIXED_LEN_BYTE_ARRAY ? 42 : 0;
      PrimitiveType expected = new PrimitiveType(
          Repetition.OPTIONAL, type, len, name, null, null, null, ColumnOrder.typeDefined());
      PrimitiveType built = Types.optional(type)
          .length(len)
          .columnOrder(ColumnOrder.typeDefined())
          .named(name);
      assertThat(built).isEqualTo(expected);
    }
  }

  @Test
  public void testTypeConstructionWithUnsupportedColumnOrder() {
    assertThatThrownBy(() -> Types.optional(INT96)
            .columnOrder(ColumnOrder.typeDefined())
            .named("int96_unsupported"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The column order TYPE_DEFINED_ORDER is not supported by INT96");
    assertThatThrownBy(() -> Types.optional(FIXED_LEN_BYTE_ARRAY)
            .length(12)
            .as(INTERVAL)
            .columnOrder(ColumnOrder.typeDefined())
            .named("interval_unsupported"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The column order TYPE_DEFINED_ORDER is not supported by FIXED_LEN_BYTE_ARRAY (INTERVAL)");
  }

  @Test
  public void testDecimalLogicalType() {
    PrimitiveType expected =
        new PrimitiveType(REQUIRED, BINARY, "aDecimal", LogicalTypeAnnotation.decimalType(3, 4));
    PrimitiveType actual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.decimalType(3, 4))
        .named("aDecimal");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testDecimalLogicalTypeWithDeprecatedScale() {
    PrimitiveType expected =
        new PrimitiveType(REQUIRED, BINARY, "aDecimal", LogicalTypeAnnotation.decimalType(3, 4));
    PrimitiveType actual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.decimalType(3, 4))
        .scale(3)
        .named("aDecimal");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testDecimalLogicalTypeWithDeprecatedPrecision() {
    PrimitiveType expected =
        new PrimitiveType(REQUIRED, BINARY, "aDecimal", LogicalTypeAnnotation.decimalType(3, 4));
    PrimitiveType actual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.decimalType(3, 4))
        .precision(4)
        .named("aDecimal");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testTimestampLogicalTypeWithUTCParameter() {
    PrimitiveType utcMillisExpected = new PrimitiveType(REQUIRED, INT64, "aTimestamp", timestampType(true, MILLIS));
    PrimitiveType nonUtcMillisExpected =
        new PrimitiveType(REQUIRED, INT64, "aTimestamp", timestampType(false, MILLIS));
    PrimitiveType utcMicrosExpected = new PrimitiveType(REQUIRED, INT64, "aTimestamp", timestampType(true, MICROS));
    PrimitiveType nonUtcMicrosExpected =
        new PrimitiveType(REQUIRED, INT64, "aTimestamp", timestampType(false, MICROS));

    PrimitiveType utcMillisActual =
        Types.required(INT64).as(timestampType(true, MILLIS)).named("aTimestamp");
    PrimitiveType nonUtcMillisActual =
        Types.required(INT64).as(timestampType(false, MILLIS)).named("aTimestamp");
    PrimitiveType utcMicrosActual =
        Types.required(INT64).as(timestampType(true, MICROS)).named("aTimestamp");
    PrimitiveType nonUtcMicrosActual =
        Types.required(INT64).as(timestampType(false, MICROS)).named("aTimestamp");

    assertThat(utcMillisActual).isEqualTo(utcMillisExpected);
    assertThat(nonUtcMillisActual).isEqualTo(nonUtcMillisExpected);
    assertThat(utcMicrosActual).isEqualTo(utcMicrosExpected);
    assertThat(nonUtcMicrosActual).isEqualTo(nonUtcMicrosExpected);
  }

  @Test
  public void testTimestampFlba12LogicalType() {
    for (TimeUnit unit : TimeUnit.values()) {
      // FLBA(12) with TIMESTAMP annotation is valid.
      Types.required(FIXED_LEN_BYTE_ARRAY)
          .length(12)
          .as(timestampType(true, unit))
          .named("ts");
      // Other FLBA lengths must be rejected.
      assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
              .length(11)
              .as(timestampType(true, unit))
              .named("ts"))
          .isInstanceOf(IllegalStateException.class);
    }
  }

  @Test
  public void testVariantLogicalType() {
    byte specVersion = 1;
    String name = "variant_field";
    GroupType variantExpected = new GroupType(
        REQUIRED,
        name,
        LogicalTypeAnnotation.variantType(specVersion),
        new PrimitiveType(REQUIRED, BINARY, "metadata"),
        new PrimitiveType(REQUIRED, BINARY, "value"));

    GroupType variantActual = Types.buildGroup(REQUIRED)
        .addFields(
            Types.required(BINARY).named("metadata"),
            Types.required(BINARY).named("value"))
        .as(LogicalTypeAnnotation.variantType(specVersion))
        .named(name);

    assertThat(variantActual).isEqualTo(variantExpected);
  }

  @Test
  public void testVariantLogicalTypeWithShredded() {
    byte specVersion = 1;
    String name = "variant_field";
    GroupType variantExpected = new GroupType(
        REQUIRED,
        name,
        LogicalTypeAnnotation.variantType(specVersion),
        new PrimitiveType(REQUIRED, BINARY, "metadata"),
        new PrimitiveType(OPTIONAL, BINARY, "value"),
        new PrimitiveType(OPTIONAL, BINARY, "typed_value", LogicalTypeAnnotation.stringType()));

    GroupType variantActual = Types.buildGroup(REQUIRED)
        .addFields(
            Types.required(BINARY).named("metadata"),
            Types.optional(BINARY).named("value"),
            Types.optional(BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("typed_value"))
        .as(LogicalTypeAnnotation.variantType(specVersion))
        .named(name);

    assertThat(variantActual).isEqualTo(variantExpected);
  }

  @Test
  public void testDecimalLogicalTypeWithDeprecatedScaleMismatch() {
    assertThatThrownBy(() -> Types.required(BINARY)
            .as(LogicalTypeAnnotation.decimalType(3, 4))
            .scale(4)
            .named("aDecimal"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Decimal scale should match with the scale of the logical type. Expected: 3, but was: 4");
  }

  @Test
  public void testDecimalLogicalTypeWithDeprecatedPrecisionMismatch() {
    assertThatThrownBy(() -> Types.required(BINARY)
            .as(LogicalTypeAnnotation.decimalType(3, 4))
            .precision(5)
            .named("aDecimal"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Decimal precision should match with the precision of the logical type. Expected: 4, but was: 5");
  }

  @Test
  public void testGeometryLogicalType() {
    // Test with default CRS
    PrimitiveType defaultCrsExpected =
        new PrimitiveType(REQUIRED, BINARY, "aGeometry", LogicalTypeAnnotation.geometryType("OGC:CRS84"));
    PrimitiveType defaultCrsActual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.geometryType("OGC:CRS84"))
        .named("aGeometry");
    assertThat(defaultCrsActual).isEqualTo(defaultCrsExpected);

    // Test with custom CRS
    PrimitiveType customCrsExpected =
        new PrimitiveType(REQUIRED, BINARY, "aGeometry", LogicalTypeAnnotation.geometryType("EPSG:4326"));
    PrimitiveType customCrsActual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.geometryType("EPSG:4326"))
        .named("aGeometry");
    assertThat(customCrsActual).isEqualTo(customCrsExpected);

    // Test with optional repetition
    PrimitiveType optionalGeometryExpected =
        new PrimitiveType(OPTIONAL, BINARY, "aGeometry", LogicalTypeAnnotation.geometryType("OGC:CRS84"));
    PrimitiveType optionalGeometryActual = Types.optional(BINARY)
        .as(LogicalTypeAnnotation.geometryType("OGC:CRS84"))
        .named("aGeometry");
    assertThat(optionalGeometryActual).isEqualTo(optionalGeometryExpected);
  }

  @Test
  public void testGeographyLogicalType() {
    // Test with default CRS and no edge algorithm
    PrimitiveType defaultCrsExpected = new PrimitiveType(
        REQUIRED, BINARY, "aGeography", LogicalTypeAnnotation.geographyType("OGC:CRS84", null));
    PrimitiveType defaultCrsActual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.geographyType("OGC:CRS84", null))
        .named("aGeography");
    assertThat(defaultCrsActual).isEqualTo(defaultCrsExpected);

    // Test with custom CRS and no edge algorithm
    PrimitiveType customCrsExpected = new PrimitiveType(
        REQUIRED, BINARY, "aGeography", LogicalTypeAnnotation.geographyType("EPSG:4326", null));
    PrimitiveType customCrsActual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.geographyType("EPSG:4326", null))
        .named("aGeography");
    assertThat(customCrsActual).isEqualTo(customCrsExpected);

    // Test with custom CRS and edge algorithm
    EdgeInterpolationAlgorithm greatCircle = EdgeInterpolationAlgorithm.SPHERICAL;
    PrimitiveType customCrsWithEdgeAlgorithmExpected = new PrimitiveType(
        REQUIRED, BINARY, "aGeography", LogicalTypeAnnotation.geographyType("EPSG:4326", greatCircle));
    PrimitiveType customCrsWithEdgeAlgorithmActual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.geographyType("EPSG:4326", greatCircle))
        .named("aGeography");
    assertThat(customCrsWithEdgeAlgorithmActual).isEqualTo(customCrsWithEdgeAlgorithmExpected);

    // Test with optional repetition
    PrimitiveType optionalGeographyExpected = new PrimitiveType(
        OPTIONAL, BINARY, "aGeography", LogicalTypeAnnotation.geographyType("OGC:CRS84", null));
    PrimitiveType optionalGeographyActual = Types.optional(BINARY)
        .as(LogicalTypeAnnotation.geographyType("OGC:CRS84", null))
        .named("aGeography");
    assertThat(optionalGeographyActual).isEqualTo(optionalGeographyExpected);
  }

  @Test
  public void testGeographyLogicalTypeWithoutEdgeInterpolationAlgorithm() {
    // Test with default CRS and no edge algorithm
    PrimitiveType defaultCrsExpected =
        new PrimitiveType(REQUIRED, BINARY, "aGeography", LogicalTypeAnnotation.geographyType());
    PrimitiveType defaultCrsActual =
        Types.required(BINARY).as(LogicalTypeAnnotation.geographyType()).named("aGeography");
    assertThat(defaultCrsActual).isEqualTo(defaultCrsExpected);

    // Test with custom CRS and no edge algorithm
    PrimitiveType customCrsExpected = new PrimitiveType(
        REQUIRED, BINARY, "aGeography", LogicalTypeAnnotation.geographyType("EPSG:4326", null));
    PrimitiveType customCrsActual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.geographyType("EPSG:4326", null))
        .named("aGeography");
    assertThat(customCrsActual).isEqualTo(customCrsExpected);

    // Test with custom CRS and edge algorithm
    PrimitiveType customCrsWithEdgeAlgorithmExpected = new PrimitiveType(
        REQUIRED, BINARY, "aGeography", LogicalTypeAnnotation.geographyType("EPSG:4326", null));
    PrimitiveType customCrsWithEdgeAlgorithmActual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.geographyType("EPSG:4326", null))
        .named("aGeography");
    assertThat(customCrsWithEdgeAlgorithmActual).isEqualTo(customCrsWithEdgeAlgorithmExpected);

    // Test with optional repetition
    PrimitiveType optionalGeographyExpected =
        new PrimitiveType(OPTIONAL, BINARY, "aGeography", LogicalTypeAnnotation.geographyType());
    PrimitiveType optionalGeographyActual =
        Types.optional(BINARY).as(LogicalTypeAnnotation.geographyType()).named("aGeography");
    assertThat(optionalGeographyActual).isEqualTo(optionalGeographyExpected);
  }
}
