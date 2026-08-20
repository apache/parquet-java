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

import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.float16Type;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.jupiter.api.Test;

public class TestTypeBuildersWithLogicalTypes {
  @Test
  public void testGroupTypeConstruction() {
    PrimitiveType f1 = Types.required(BINARY).as(stringType()).named("f1");
    PrimitiveType f2 = Types.required(INT32).named("f2");
    PrimitiveType f3 = Types.optional(INT32).named("f3");
    String name = "group";
    for (Repetition repetition : Repetition.values()) {
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
  public void testDecimalAnnotation() {
    // int32 primitive type
    MessageType expected = new MessageType(
        "DecimalMessage", new PrimitiveType(REQUIRED, INT32, 0, "aDecimal", decimalType(2, 9), null));
    MessageType builderType = Types.buildMessage()
        .required(INT32)
        .as(decimalType(2, 9))
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);
    // int64 primitive type
    expected = new MessageType(
        "DecimalMessage", new PrimitiveType(REQUIRED, INT64, 0, "aDecimal", decimalType(2, 18), null));
    builderType = Types.buildMessage()
        .required(INT64)
        .as(decimalType(2, 18))
        .precision(18)
        .scale(2)
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);
    // binary primitive type
    expected = new MessageType(
        "DecimalMessage", new PrimitiveType(REQUIRED, BINARY, 0, "aDecimal", decimalType(2, 9), null));
    builderType = Types.buildMessage()
        .required(BINARY)
        .as(decimalType(2, 9))
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);
    // fixed primitive type
    expected = new MessageType(
        "DecimalMessage",
        new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, 4, "aDecimal", decimalType(2, 9), null));
    builderType = Types.buildMessage()
        .required(FIXED_LEN_BYTE_ARRAY)
        .length(4)
        .as(decimalType(2, 9))
        .named("aDecimal")
        .named("DecimalMessage");
    assertThat(builderType).isEqualTo(expected);
  }

  @Test
  public void testDecimalAnnotationPrecisionScaleBound() {
    String expectedMessage = "Invalid DECIMAL scale: 4 cannot be greater than precision: 3";
    assertThatThrownBy(() -> Types.buildMessage()
            .required(INT32)
            .as(decimalType(4, 3))
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
    assertThatThrownBy(() -> Types.buildMessage()
            .required(INT64)
            .as(decimalType(4, 3))
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
    assertThatThrownBy(() -> Types.buildMessage()
            .required(BINARY)
            .as(decimalType(4, 3))
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
    assertThatThrownBy(() -> Types.buildMessage()
            .required(FIXED_LEN_BYTE_ARRAY)
            .length(7)
            .as(decimalType(4, 3))
            .named("aDecimal")
            .named("DecimalMessage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  @Test
  public void testDecimalAnnotationLengthCheck() {
    // maximum precision for 4 bytes is 9
    assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(4)
            .as(decimalType(2, 10))
            .named("aDecimal"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("FIXED(4) cannot store 10 digits (max 9)");
    assertThatThrownBy(() -> Types.required(INT32).as(decimalType(2, 10)).named("aDecimal"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("INT32 cannot store 10 digits (max 9)");
    // maximum precision for 8 bytes is 19
    assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(8)
            .as(decimalType(4, 19))
            .named("aDecimal"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("FIXED(8) cannot store 19 digits (max 18)");
    assertThatThrownBy(() ->
            Types.required(INT64).length(8).as(decimalType(4, 19)).named("aDecimal"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("INT64 cannot store 19 digits (max 18)");
  }

  @Test
  public void testDECIMALAnnotationRejectsUnsupportedTypes() {
    PrimitiveTypeName[] unsupported = new PrimitiveTypeName[] {BOOLEAN, INT96, DOUBLE, FLOAT};
    for (final PrimitiveTypeName type : unsupported) {
      assertThatThrownBy(() -> Types.required(type).as(decimalType(2, 9)).named("d"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("DECIMAL can only annotate INT32, INT64, BINARY, and FIXED");
    }
  }

  @Test
  public void testBinaryAnnotations() {
    LogicalTypeAnnotation[] types = new LogicalTypeAnnotation[] {stringType(), jsonType(), bsonType()};
    for (final LogicalTypeAnnotation logicalType : types) {
      PrimitiveType expected = new PrimitiveType(REQUIRED, BINARY, "col", logicalType);
      PrimitiveType string = Types.required(BINARY).as(logicalType).named("col");
      assertThat(string).isEqualTo(expected);
    }
  }

  @Test
  public void testFloat16Annotations() {
    LogicalTypeAnnotation type = float16Type();
    PrimitiveType expected = new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, 2, "col", type, null);
    PrimitiveType string =
        Types.required(FIXED_LEN_BYTE_ARRAY).as(type).length(2).named("col");
    assertThat(string).isEqualTo(expected);
  }

  @Test
  public void testBinaryAnnotationsRejectsNonBinary() {
    LogicalTypeAnnotation[] types =
        new LogicalTypeAnnotation[] {stringType(), jsonType(), bsonType(), float16Type()};
    for (final LogicalTypeAnnotation logicalType : types) {
      PrimitiveTypeName[] nonBinary = new PrimitiveTypeName[] {BOOLEAN, INT32, INT64, INT96, DOUBLE, FLOAT};
      for (final PrimitiveTypeName type : nonBinary) {
        String expectedMessage = logicalType.equals(float16Type())
            ? "FLOAT16 can only annotate FIXED_LEN_BYTE_ARRAY(2)"
            : logicalType + " can only annotate BINARY";
        assertThatThrownBy(() -> Types.required(type).as(logicalType).named("col"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage(expectedMessage);
      }
      String fixedMessage = logicalType.equals(float16Type())
          ? "FLOAT16 can only annotate FIXED_LEN_BYTE_ARRAY(2)"
          : logicalType + " can only annotate BINARY";
      assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
              .length(1)
              .as(logicalType)
              .named("col"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(fixedMessage);
    }
  }

  @Test
  public void testInt32Annotations() {
    LogicalTypeAnnotation[] types = new LogicalTypeAnnotation[] {
      dateType(), timeType(true, MILLIS), timeType(false, MILLIS),
      intType(8, false), intType(16, false), intType(32, false),
      intType(8, true), intType(16, true), intType(32, true)
    };
    for (LogicalTypeAnnotation logicalType : types) {
      PrimitiveType expected = new PrimitiveType(REQUIRED, INT32, "col", logicalType);
      PrimitiveType date = Types.required(INT32).as(logicalType).named("col");
      assertThat(date).isEqualTo(expected);
    }
  }

  @Test
  public void testInt32AnnotationsRejectNonInt32() {
    LogicalTypeAnnotation[] types = new LogicalTypeAnnotation[] {
      dateType(), timeType(true, MILLIS), timeType(false, MILLIS),
      intType(8, false), intType(16, false), intType(32, false),
      intType(8, true), intType(16, true), intType(32, true)
    };
    for (final LogicalTypeAnnotation logicalType : types) {
      PrimitiveTypeName[] nonInt32 = new PrimitiveTypeName[] {BOOLEAN, INT64, INT96, DOUBLE, FLOAT, BINARY};
      for (final PrimitiveTypeName type : nonInt32) {
        assertThatThrownBy(() -> Types.required(type).as(logicalType).named("col"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage(logicalType + " can only annotate INT32");
      }
      assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
              .length(1)
              .as(logicalType)
              .named("col"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(logicalType + " can only annotate INT32");
    }
  }

  @Test
  public void testInt64Annotations() {
    LogicalTypeAnnotation[] types = new LogicalTypeAnnotation[] {
      timeType(true, MICROS), timeType(false, MICROS),
      timeType(true, NANOS), timeType(false, NANOS),
      timestampType(true, MILLIS), timestampType(false, MILLIS),
      timestampType(true, MICROS), timestampType(false, MICROS),
      timestampType(true, NANOS), timestampType(false, NANOS),
      intType(64, true), intType(64, false)
    };
    for (LogicalTypeAnnotation logicalType : types) {
      PrimitiveType expected = new PrimitiveType(REQUIRED, INT64, "col", logicalType);
      PrimitiveType date = Types.required(INT64).as(logicalType).named("col");
      assertThat(date).isEqualTo(expected);
    }
  }

  @Test
  public void testInt64AnnotationsRejectNonInt64() {
    LogicalTypeAnnotation[] types = new LogicalTypeAnnotation[] {
      timeType(true, MICROS), timeType(false, MICROS),
      timeType(true, NANOS), timeType(false, NANOS),
      timestampType(true, MILLIS), timestampType(false, MILLIS),
      timestampType(true, MICROS), timestampType(false, MICROS),
      timestampType(true, NANOS), timestampType(false, NANOS),
      intType(64, true), intType(64, false)
    };
    for (final LogicalTypeAnnotation logicalType : types) {
      PrimitiveTypeName[] nonInt64 = new PrimitiveTypeName[] {BOOLEAN, INT32, INT96, DOUBLE, FLOAT, BINARY};
      for (final PrimitiveTypeName type : nonInt64) {
        assertThatThrownBy(() -> Types.required(type).as(logicalType).named("col"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage(logicalType + " can only annotate INT64");
      }
      assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
              .length(1)
              .as(logicalType)
              .named("col"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(logicalType + " can only annotate INT64");
    }
  }

  @Test
  public void testIntervalAnnotationRejectsNonFixed() {
    PrimitiveTypeName[] nonFixed = new PrimitiveTypeName[] {BOOLEAN, INT32, INT64, INT96, DOUBLE, FLOAT, BINARY};
    for (final PrimitiveTypeName type : nonFixed) {
      assertThatThrownBy(() -> Types.required(type)
              .as(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance())
              .named("interval"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)");
    }
  }

  @Test
  public void testIntervalAnnotationRejectsNonFixed12() {
    assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(11)
            .as(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance())
            .named("interval"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)");
  }

  @Test
  public void testTypeConstructionWithUnsupportedColumnOrder() {
    assertThatThrownBy(() -> Types.optional(INT96)
            .columnOrder(ColumnOrder.typeDefined())
            .named("int96_unsupported"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The column order TYPE_DEFINED_ORDER is not supported by INT96");
    assertThatThrownBy(() -> Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(12)
            .as(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance())
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
  public void testUUIDLogicalType() {
    assertThat(Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(16)
            .as(uuidType())
            .named("uuid_field"))
        .asString()
        .isEqualTo("required fixed_len_byte_array(16) uuid_field (UUID)");

    assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(10)
            .as(uuidType())
            .named("uuid_field")
            .toString())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("UUID can only annotate FIXED_LEN_BYTE_ARRAY(16)");
    assertThatThrownBy(() -> Types.required(BINARY)
            .as(uuidType())
            .named("uuid_field")
            .toString())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("UUID can only annotate FIXED_LEN_BYTE_ARRAY(16)");
  }

  @Test
  public void testFloat16LogicalType() {
    assertThat(Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(2)
            .as(float16Type())
            .named("float16_field"))
        .asString()
        .isEqualTo("required fixed_len_byte_array(2) float16_field (FLOAT16)");

    assertThatThrownBy(() -> Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(10)
            .as(float16Type())
            .named("float16_field")
            .toString())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("FLOAT16 can only annotate FIXED_LEN_BYTE_ARRAY(2)");
    assertThatThrownBy(() -> Types.required(BINARY)
            .as(float16Type())
            .named("float16_field")
            .toString())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("FLOAT16 can only annotate FIXED_LEN_BYTE_ARRAY(2)");
  }

  @Test
  public void testVariantLogicalType() {
    byte specVersion = 1;
    String name = "variant_field";
    GroupType variant = new GroupType(
        REQUIRED,
        name,
        LogicalTypeAnnotation.variantType(specVersion),
        Types.required(BINARY).named("metadata"),
        Types.required(BINARY).named("value"));

    assertThat(variant)
        .asString()
        .isEqualTo("required group variant_field (VARIANT(1)) {\n"
            + "  required binary metadata;\n"
            + "  required binary value;\n"
            + "}");

    LogicalTypeAnnotation annotation = variant.getLogicalTypeAnnotation();
    assertThat(annotation.getType()).isEqualTo(LogicalTypeAnnotation.LogicalTypeToken.VARIANT);
    assertThat(annotation.toOriginalType()).isNull();
    assertThat(annotation).isInstanceOf(LogicalTypeAnnotation.VariantLogicalTypeAnnotation.class);
    assertThat(((LogicalTypeAnnotation.VariantLogicalTypeAnnotation) annotation).getSpecVersion())
        .isEqualTo(specVersion);
  }

  @Test
  public void testVariantLogicalTypeWithShredded() {
    byte specVersion = 1;

    String name = "variant_field";
    GroupType variant = new GroupType(
        REQUIRED,
        name,
        LogicalTypeAnnotation.variantType(specVersion),
        Types.required(BINARY).named("metadata"),
        Types.optional(BINARY).named("value"),
        Types.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("typed_value"));

    assertThat(variant)
        .asString()
        .isEqualTo("required group variant_field (VARIANT(1)) {\n"
            + "  required binary metadata;\n"
            + "  optional binary value;\n"
            + "  optional binary typed_value (STRING);\n"
            + "}");

    LogicalTypeAnnotation annotation = variant.getLogicalTypeAnnotation();
    assertThat(annotation.getType()).isEqualTo(LogicalTypeAnnotation.LogicalTypeToken.VARIANT);
    assertThat(annotation.toOriginalType()).isNull();
    assertThat(annotation).isInstanceOf(LogicalTypeAnnotation.VariantLogicalTypeAnnotation.class);
    assertThat(((LogicalTypeAnnotation.VariantLogicalTypeAnnotation) annotation).getSpecVersion())
        .isEqualTo(specVersion);
  }

  @Test
  public void testFileLogicalTypeUriOnly() {
    String name = "file_field";
    GroupType file = new GroupType(
        REQUIRED,
        name,
        LogicalTypeAnnotation.fileType(),
        Types.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("uri"));

    assertThat(file.toString())
        .isEqualTo("required group file_field (FILE) {\n" + "  optional binary uri (STRING);\n" + "}");

    LogicalTypeAnnotation annotation = file.getLogicalTypeAnnotation();
    assertThat(annotation.getType()).isEqualTo(LogicalTypeAnnotation.LogicalTypeToken.FILE);
    assertThat(annotation.toOriginalType()).isNull();
    assertThat(annotation).isInstanceOf(LogicalTypeAnnotation.FileLogicalTypeAnnotation.class);
  }

  @Test
  public void testFileLogicalTypeAllFields() {
    String name = "file_field";
    GroupType file = Types.requiredGroup()
        .as(LogicalTypeAnnotation.fileType())
        .optional(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("uri")
        .optional(INT64)
        .named("offset")
        .optional(INT64)
        .named("size")
        .optional(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("content_type")
        .optional(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("checksum")
        .optional(BINARY)
        .named("inline")
        .named(name);

    LogicalTypeAnnotation annotation = file.getLogicalTypeAnnotation();
    assertThat(annotation).isInstanceOf(LogicalTypeAnnotation.FileLogicalTypeAnnotation.class);
    assertThat(file.getFieldCount()).isEqualTo(6);
    assertThat(file.getType("uri").getName()).isEqualTo("uri");
    assertThat(file.getType("offset").getName()).isEqualTo("offset");
    assertThat(file.getType("size").getName()).isEqualTo("size");
    assertThat(file.getType("content_type").getName()).isEqualTo("content_type");
    assertThat(file.getType("checksum").getName()).isEqualTo("checksum");
    assertThat(file.getType("inline").getName()).isEqualTo("inline");
  }

  @Test
  public void testFileLogicalTypeInlineOnly() {
    // Every field is optional, so an inline-only group is valid (spec inline case).
    GroupType file = Types.requiredGroup()
        .as(LogicalTypeAnnotation.fileType())
        .optional(BINARY)
        .named("inline")
        .named("inline_file");

    assertThat(file.getLogicalTypeAnnotation()).isInstanceOf(LogicalTypeAnnotation.FileLogicalTypeAnnotation.class);
    assertThat(file.getFieldCount()).isEqualTo(1);
    assertThat(file.getType("inline").getName()).isEqualTo("inline");
  }

  @Test
  public void testFileLogicalTypeSelfReference() {
    // A self-reference omits 'uri' and locates bytes within the current file via offset/size.
    // A schema that permits self-references must declare 'inline' as the storage-inheritance
    // reference point.
    GroupType file = Types.requiredGroup()
        .as(LogicalTypeAnnotation.fileType())
        .optional(INT64)
        .named("offset")
        .optional(INT64)
        .named("size")
        .optional(BINARY)
        .named("inline")
        .named("self_ref_file");

    assertThat(file.getLogicalTypeAnnotation()).isInstanceOf(LogicalTypeAnnotation.FileLogicalTypeAnnotation.class);
    assertThat(file.getFieldCount()).isEqualTo(3);
  }

  @Test
  public void testFileLogicalTypeOffsetRequiresInline() {
    // A schema that permits self-references (declares 'offset' but not 'uri') must declare 'inline'
    // as the reference point for storage inheritance. A group declaring 'offset'/'size' with
    // neither 'uri' nor 'inline' is rejected at build time.
    assertThatThrownBy(() -> Types.requiredGroup()
            .as(LogicalTypeAnnotation.fileType())
            .optional(INT64)
            .named("offset")
            .optional(INT64)
            .named("size")
            .named("self_ref_without_inline"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFileLogicalTypeExternalRangedReferenceWithoutInline() {
    // An external ranged reference declares 'uri' + 'offset' + 'size' to point at a byte range of
    // an external file. Because 'uri' is declared, the schema is treated as an external-reference
    // schema and is not required to declare 'inline', even though it declares 'offset'.
    GroupType file = Types.requiredGroup()
        .as(LogicalTypeAnnotation.fileType())
        .optional(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("uri")
        .optional(INT64)
        .named("offset")
        .optional(INT64)
        .named("size")
        .named("external_ranged_file");

    assertThat(file.getLogicalTypeAnnotation()).isInstanceOf(LogicalTypeAnnotation.FileLogicalTypeAnnotation.class);
    assertThat(file.getFieldCount()).isEqualTo(3);
  }

  @Test
  public void testFileLogicalTypeMetadataOnlyRejected() {
    // Per the spec resolution table, a value resolves to bytes only via 'inline', 'uri', or
    // 'offset'. A group declaring only metadata fields can never produce a resolvable value.
    assertThatThrownBy(() -> Types.requiredGroup()
            .as(LogicalTypeAnnotation.fileType())
            .optional(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("content_type")
            .optional(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("checksum")
            .named("file_metadata_only"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFileLogicalTypeSizeOnlyRejected() {
    // 'size' alone never resolves to bytes (spec resolution table), so a size-only group is
    // rejected: it declares no locator ('inline', 'uri', or 'offset').
    assertThatThrownBy(() -> Types.requiredGroup()
            .as(LogicalTypeAnnotation.fileType())
            .optional(INT64)
            .named("size")
            .named("file_size_only"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFileLogicalTypeOffsetRequiresSize() {
    // The spec requires 'size' whenever 'offset' is set, so a group declaring 'offset'
    // without 'size' can never produce a valid value and is rejected at build time.
    assertThatThrownBy(() -> Types.requiredGroup()
            .as(LogicalTypeAnnotation.fileType())
            .optional(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("uri")
            .optional(INT64)
            .named("offset")
            .named("file_offset_without_size"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFileLogicalTypeOffsetWithSize() {
    // 'offset' accompanied by 'size' is valid.
    GroupType file = Types.requiredGroup()
        .as(LogicalTypeAnnotation.fileType())
        .optional(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("uri")
        .optional(INT64)
        .named("offset")
        .optional(INT64)
        .named("size")
        .named("file_offset_with_size");

    assertThat(file.getLogicalTypeAnnotation()).isInstanceOf(LogicalTypeAnnotation.FileLogicalTypeAnnotation.class);
    assertThat(file.getFieldCount()).isEqualTo(3);
  }

  @Test
  public void testFileLogicalTypeSizeWithoutOffset() {
    // 'uri' + 'size' (without 'offset') is valid: an external reference to '[0, size)'.
    GroupType file = Types.requiredGroup()
        .as(LogicalTypeAnnotation.fileType())
        .optional(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("uri")
        .optional(INT64)
        .named("size")
        .named("file_size_without_offset");

    assertThat(file.getLogicalTypeAnnotation()).isInstanceOf(LogicalTypeAnnotation.FileLogicalTypeAnnotation.class);
    assertThat(file.getFieldCount()).isEqualTo(2);
  }

  @Test
  public void testFileLogicalTypeRejectsUnrecognizedField() {
    assertThatThrownBy(() -> Types.requiredGroup()
            .as(LogicalTypeAnnotation.fileType())
            .optional(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("uri")
            .optional(BINARY)
            .named("unknown_field")
            .named("file_with_bad_field"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFileLogicalTypeRejectsRequiredField() {
    // All FILE fields must have OPTIONAL repetition under the current spec.
    assertThatThrownBy(() -> Types.requiredGroup()
            .as(LogicalTypeAnnotation.fileType())
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("uri")
            .named("file_with_required_uri"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFileLogicalTypeRejectsGroupField() {
    // FILE fields must be primitives, not nested groups.
    assertThatThrownBy(() -> Types.requiredGroup()
            .as(LogicalTypeAnnotation.fileType())
            .optionalGroup()
            .optional(BINARY)
            .named("nested")
            .named("uri")
            .named("file_with_group_field"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFileLogicalTypeRejectsWrongStringPhysicalType() {
    // 'uri' must be a STRING (BINARY annotated as STRING); an INT64 is rejected.
    assertThatThrownBy(() -> Types.requiredGroup()
            .as(LogicalTypeAnnotation.fileType())
            .optional(INT64)
            .named("uri")
            .named("file_uri_wrong_type"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFileLogicalTypeRejectsUnannotatedStringField() {
    // A STRING field must carry the STRING logical annotation; plain BINARY is rejected.
    assertThatThrownBy(() -> Types.requiredGroup()
            .as(LogicalTypeAnnotation.fileType())
            .optional(BINARY)
            .named("uri")
            .named("file_uri_unannotated"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFileLogicalTypeRejectsWrongInt64PhysicalType() {
    // 'offset' and 'size' must be INT64; an INT32 is rejected.
    assertThatThrownBy(() -> Types.requiredGroup()
            .as(LogicalTypeAnnotation.fileType())
            .optional(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("uri")
            .optional(INT32)
            .named("size")
            .named("file_size_wrong_type"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFileLogicalTypeRejectsWrongInlinePhysicalType() {
    // 'inline' must be a BYTE_ARRAY (BINARY); an INT64 is rejected.
    assertThatThrownBy(() -> Types.requiredGroup()
            .as(LogicalTypeAnnotation.fileType())
            .optional(INT64)
            .named("inline")
            .named("file_inline_wrong_type"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
