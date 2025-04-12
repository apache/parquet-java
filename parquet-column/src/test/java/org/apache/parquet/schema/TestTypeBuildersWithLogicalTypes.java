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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Assert;
import org.junit.Test;

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
      Assert.assertEquals(expected, built);

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
      Assert.assertEquals(expected, built);
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
    Assert.assertEquals(expected, builderType);
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
    Assert.assertEquals(expected, builderType);
    // binary primitive type
    expected = new MessageType(
        "DecimalMessage", new PrimitiveType(REQUIRED, BINARY, 0, "aDecimal", decimalType(2, 9), null));
    builderType = Types.buildMessage()
        .required(BINARY)
        .as(decimalType(2, 9))
        .named("aDecimal")
        .named("DecimalMessage");
    Assert.assertEquals(expected, builderType);
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
    Assert.assertEquals(expected, builderType);
  }

  @Test
  public void testDecimalAnnotationPrecisionScaleBound() {
    assertThrows(
        "Should reject scale greater than precision", IllegalArgumentException.class, () -> Types.buildMessage()
            .required(INT32)
            .as(decimalType(4, 3))
            .named("aDecimal")
            .named("DecimalMessage"));
    assertThrows(
        "Should reject scale greater than precision", IllegalArgumentException.class, () -> Types.buildMessage()
            .required(INT64)
            .as(decimalType(4, 3))
            .named("aDecimal")
            .named("DecimalMessage"));
    assertThrows(
        "Should reject scale greater than precision", IllegalArgumentException.class, () -> Types.buildMessage()
            .required(BINARY)
            .as(decimalType(4, 3))
            .named("aDecimal")
            .named("DecimalMessage"));
    assertThrows(
        "Should reject scale greater than precision", IllegalArgumentException.class, () -> Types.buildMessage()
            .required(FIXED_LEN_BYTE_ARRAY)
            .length(7)
            .as(decimalType(4, 3))
            .named("aDecimal")
            .named("DecimalMessage"));
  }

  @Test
  public void testDecimalAnnotationLengthCheck() {
    // maximum precision for 4 bytes is 9
    assertThrows("should reject precision 10 with length 4", IllegalStateException.class, () -> Types.required(
            FIXED_LEN_BYTE_ARRAY)
        .length(4)
        .as(decimalType(2, 10))
        .named("aDecimal"));
    assertThrows(
        "should reject precision 10 with length 4",
        IllegalStateException.class,
        () -> Types.required(INT32).as(decimalType(2, 10)).named("aDecimal"));
    // maximum precision for 8 bytes is 19
    assertThrows("should reject precision 19 with length 8", IllegalStateException.class, () -> Types.required(
            FIXED_LEN_BYTE_ARRAY)
        .length(8)
        .as(decimalType(4, 19))
        .named("aDecimal"));
    assertThrows(
        "should reject precision 19 with length 8",
        IllegalStateException.class,
        () -> Types.required(INT64).length(8).as(decimalType(4, 19)).named("aDecimal"));
  }

  @Test
  public void testDECIMALAnnotationRejectsUnsupportedTypes() {
    PrimitiveTypeName[] unsupported = new PrimitiveTypeName[] {BOOLEAN, INT96, DOUBLE, FLOAT};
    for (final PrimitiveTypeName type : unsupported) {
      assertThrows(
          "Should reject non-binary type: " + type,
          IllegalStateException.class,
          () -> Types.required(type).as(decimalType(2, 9)).named("d"));
    }
  }

  @Test
  public void testBinaryAnnotations() {
    LogicalTypeAnnotation[] types = new LogicalTypeAnnotation[] {stringType(), jsonType(), bsonType()};
    for (final LogicalTypeAnnotation logicalType : types) {
      PrimitiveType expected = new PrimitiveType(REQUIRED, BINARY, "col", logicalType);
      PrimitiveType string = Types.required(BINARY).as(logicalType).named("col");
      Assert.assertEquals(expected, string);
    }
  }

  @Test
  public void testFloat16Annotations() {
    LogicalTypeAnnotation type = float16Type();
    PrimitiveType expected = new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, 2, "col", type, null);
    PrimitiveType string =
        Types.required(FIXED_LEN_BYTE_ARRAY).as(type).length(2).named("col");
    Assert.assertEquals(expected, string);
  }

  @Test
  public void testBinaryAnnotationsRejectsNonBinary() {
    LogicalTypeAnnotation[] types =
        new LogicalTypeAnnotation[] {stringType(), jsonType(), bsonType(), float16Type()};
    for (final LogicalTypeAnnotation logicalType : types) {
      PrimitiveTypeName[] nonBinary = new PrimitiveTypeName[] {BOOLEAN, INT32, INT64, INT96, DOUBLE, FLOAT};
      for (final PrimitiveTypeName type : nonBinary) {
        assertThrows(
            "Should reject non-binary type: " + type,
            IllegalStateException.class,
            () -> Types.required(type).as(logicalType).named("col"));
      }
      assertThrows(
          "Should reject non-binary type: FIXED_LEN_BYTE_ARRAY",
          IllegalStateException.class,
          () -> Types.required(FIXED_LEN_BYTE_ARRAY)
              .length(1)
              .as(logicalType)
              .named("col"));
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
      Assert.assertEquals(expected, date);
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
        assertThrows(
            "Should reject non-int32 type: " + type,
            IllegalStateException.class,
            () -> Types.required(type).as(logicalType).named("col"));
      }
      assertThrows(
          "Should reject non-int32 type: FIXED_LEN_BYTE_ARRAY",
          IllegalStateException.class,
          () -> Types.required(FIXED_LEN_BYTE_ARRAY)
              .length(1)
              .as(logicalType)
              .named("col"));
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
      Assert.assertEquals(expected, date);
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
        assertThrows("Should reject non-int64 type: " + type, IllegalStateException.class, (Callable<Type>)
            () -> Types.required(type).as(logicalType).named("col"));
      }
      assertThrows(
          "Should reject non-int64 type: FIXED_LEN_BYTE_ARRAY", IllegalStateException.class, (Callable<Type>)
              () -> Types.required(FIXED_LEN_BYTE_ARRAY)
                  .length(1)
                  .as(logicalType)
                  .named("col"));
    }
  }

  @Test
  public void testIntervalAnnotationRejectsNonFixed() {
    PrimitiveTypeName[] nonFixed = new PrimitiveTypeName[] {BOOLEAN, INT32, INT64, INT96, DOUBLE, FLOAT, BINARY};
    for (final PrimitiveTypeName type : nonFixed) {
      assertThrows(
          "Should reject non-fixed type: " + type, IllegalStateException.class, () -> Types.required(type)
              .as(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance())
              .named("interval"));
    }
  }

  @Test
  public void testIntervalAnnotationRejectsNonFixed12() {
    assertThrows("Should reject fixed with length != 12: " + 11, IllegalStateException.class, () -> Types.required(
            FIXED_LEN_BYTE_ARRAY)
        .length(11)
        .as(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance())
        .named("interval"));
  }

  @Test
  public void testTypeConstructionWithUnsupportedColumnOrder() {
    assertThrows(null, IllegalArgumentException.class, () -> Types.optional(INT96)
        .columnOrder(ColumnOrder.typeDefined())
        .named("int96_unsupported"));
    assertThrows(null, IllegalArgumentException.class, () -> Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(12)
        .as(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance())
        .columnOrder(ColumnOrder.typeDefined())
        .named("interval_unsupported"));
  }

  @Test
  public void testDecimalLogicalType() {
    PrimitiveType expected =
        new PrimitiveType(REQUIRED, BINARY, "aDecimal", LogicalTypeAnnotation.decimalType(3, 4));
    PrimitiveType actual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.decimalType(3, 4))
        .named("aDecimal");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDecimalLogicalTypeWithDeprecatedScale() {
    PrimitiveType expected =
        new PrimitiveType(REQUIRED, BINARY, "aDecimal", LogicalTypeAnnotation.decimalType(3, 4));
    PrimitiveType actual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.decimalType(3, 4))
        .scale(3)
        .named("aDecimal");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDecimalLogicalTypeWithDeprecatedPrecision() {
    PrimitiveType expected =
        new PrimitiveType(REQUIRED, BINARY, "aDecimal", LogicalTypeAnnotation.decimalType(3, 4));
    PrimitiveType actual = Types.required(BINARY)
        .as(LogicalTypeAnnotation.decimalType(3, 4))
        .precision(4)
        .named("aDecimal");
    Assert.assertEquals(expected, actual);
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

    Assert.assertEquals(utcMillisExpected, utcMillisActual);
    Assert.assertEquals(nonUtcMillisExpected, nonUtcMillisActual);
    Assert.assertEquals(utcMicrosExpected, utcMicrosActual);
    Assert.assertEquals(nonUtcMicrosExpected, nonUtcMicrosActual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDecimalLogicalTypeWithDeprecatedScaleMismatch() {
    Types.required(BINARY)
        .as(LogicalTypeAnnotation.decimalType(3, 4))
        .scale(4)
        .named("aDecimal");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDecimalLogicalTypeWithDeprecatedPrecisionMismatch() {
    Types.required(BINARY)
        .as(LogicalTypeAnnotation.decimalType(3, 4))
        .precision(5)
        .named("aDecimal");
  }

  @Test
  public void testUUIDLogicalType() {
    assertEquals(
        "required fixed_len_byte_array(16) uuid_field (UUID)",
        Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(16)
            .as(uuidType())
            .named("uuid_field")
            .toString());

    assertThrows("Should fail with invalid length", IllegalStateException.class, () -> Types.required(
            FIXED_LEN_BYTE_ARRAY)
        .length(10)
        .as(uuidType())
        .named("uuid_field")
        .toString());
    assertThrows(
        "Should fail with invalid type",
        IllegalStateException.class,
        () -> Types.required(BINARY).as(uuidType()).named("uuid_field").toString());
  }

  @Test
  public void testFloat16LogicalType() {
    assertEquals(
        "required fixed_len_byte_array(2) float16_field (FLOAT16)",
        Types.required(FIXED_LEN_BYTE_ARRAY)
            .length(2)
            .as(float16Type())
            .named("float16_field")
            .toString());

    assertThrows("Should fail with invalid length", IllegalStateException.class, () -> Types.required(
            FIXED_LEN_BYTE_ARRAY)
        .length(10)
        .as(float16Type())
        .named("float16_field")
        .toString());
    assertThrows("Should fail with invalid type", IllegalStateException.class, () -> Types.required(BINARY)
        .as(float16Type())
        .named("float16_field")
        .toString());
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

    assertEquals(
        "required group variant_field (VARIANT(1)) {\n"
            + "  required binary metadata;\n"
            + "  required binary value;\n"
            + "}",
        variant.toString());

    LogicalTypeAnnotation annotation = variant.getLogicalTypeAnnotation();
    assertEquals(LogicalTypeAnnotation.LogicalTypeToken.VARIANT, annotation.getType());
    assertNull(annotation.toOriginalType());
    assertTrue(annotation instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
    assertEquals(
        specVersion,
        ((LogicalTypeAnnotation.VariantLogicalTypeAnnotation) annotation).getSpecificationVersion());
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

    assertEquals(
        "required group variant_field (VARIANT(1)) {\n"
            + "  required binary metadata;\n"
            + "  optional binary value;\n"
            + "  optional binary typed_value (STRING);\n"
            + "}",
        variant.toString());

    LogicalTypeAnnotation annotation = variant.getLogicalTypeAnnotation();
    assertEquals(LogicalTypeAnnotation.LogicalTypeToken.VARIANT, annotation.getType());
    assertNull(annotation.toOriginalType());
    assertTrue(annotation instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation);
    assertEquals(
        specVersion,
        ((LogicalTypeAnnotation.VariantLogicalTypeAnnotation) annotation).getSpecificationVersion());
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   *
   * @param message  A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(String message, Class<? extends Exception> expected, Callable callable) {
    try {
      callable.call();
      Assert.fail("No exception was thrown (" + message + "), expected: " + expected.getName());
    } catch (Exception actual) {
      Assert.assertEquals(message, expected, actual.getClass());
    }
  }
}
