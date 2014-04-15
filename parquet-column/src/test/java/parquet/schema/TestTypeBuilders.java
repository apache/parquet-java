package parquet.schema;

import java.util.concurrent.Callable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import static parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static parquet.schema.Type.Repetition.*;

public class TestTypeBuilders {
  @Test
  public void testPaperExample() {
    MessageType expected =
        new MessageType("Document",
            new PrimitiveType(REQUIRED, INT64, "DocId"),
            new GroupType(OPTIONAL, "Links",
                new PrimitiveType(REPEATED, INT64, "Backward"),
                new PrimitiveType(REPEATED, INT64, "Forward")),
            new GroupType(REPEATED, "Name",
                new GroupType(REPEATED, "Language",
                    new PrimitiveType(REQUIRED, BINARY, "Code"),
                    new PrimitiveType(REQUIRED, BINARY, "Country")),
                new PrimitiveType(OPTIONAL, BINARY, "Url")));
    MessageType builderType = Types.buildMessage()
        .required(INT64).named("DocId")
        .optionalGroup()
            .repeated(INT64).named("Backward")
            .repeated(INT64).named("Forward")
            .named("Links")
        .repeatedGroup()
            .repeatedGroup()
                .required(BINARY).named("Code")
                .required(BINARY).named("Country")
            .named("Language")
            .optional(BINARY).named("Url")
            .named("Name")
        .named("Document");
    Assert.assertEquals(expected, builderType);
  }

  @Test
  public void testGroupTypeConstruction() {
    PrimitiveType f1 = Types.required(BINARY).as(OriginalType.UTF8).named("f1");
    PrimitiveType f2 = Types.required(INT32).named("f2");
    PrimitiveType f3 = Types.optional(INT32).named("f3");
    String name = "group";
    for (Type.Repetition repetition : Type.Repetition.values()) {
      GroupType expected = new GroupType(repetition, name,
          f1,
          new GroupType(repetition, "g1", f2, f3));
      GroupType built = Types.buildGroup(repetition)
          .addField(f1)
          .group(repetition).addFields(f2, f3).named("g1")
          .named(name);
      Assert.assertEquals(expected, built);

      switch (repetition) {
        case REQUIRED:
          built = Types.requiredGroup()
              .addField(f1)
              .requiredGroup().addFields(f2, f3).named("g1")
              .named(name);
          break;
        case OPTIONAL:
          built = Types.optionalGroup()
              .addField(f1)
              .optionalGroup().addFields(f2, f3).named("g1")
              .named(name);
          break;
        case REPEATED:
          built = Types.repeatedGroup()
              .addField(f1)
              .repeatedGroup().addFields(f2, f3).named("g1")
              .named(name);
          break;
      }
      Assert.assertEquals(expected, built);
    }
  }

  @Test
  public void testPrimitiveTypeConstruction() {
    PrimitiveTypeName[] types = new PrimitiveTypeName[] {
        BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BINARY
    };
    for (PrimitiveTypeName type : types) {
      String name = type.toString() + "_";
      for (Type.Repetition repetition : Type.Repetition.values()) {
        PrimitiveType expected = new PrimitiveType(repetition, type, name);
        PrimitiveType built = Types.primitive(type, repetition).named(name);
        Assert.assertEquals(expected, built);
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
        Assert.assertEquals(expected, built);
      }
    }
  }

  @Test
  public void testFixedTypeConstruction() {
    String name = "fixed_";
    int len = 5;
    for (Type.Repetition repetition : Type.Repetition.values()) {
      PrimitiveType expected = new PrimitiveType(
          repetition, FIXED_LEN_BYTE_ARRAY, len, name);
      PrimitiveType built = Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .length(len).named(name);
      Assert.assertEquals(expected, built);
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
      Assert.assertEquals(expected, built);
    }
  }

  @Test
  public void testEmptyGroup() {
    assertThrows("Should complain that required group is empty",
        IllegalStateException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.requiredGroup().named("g");
          }
        });
    assertThrows("Should complain that optional group is empty",
        IllegalStateException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.optionalGroup().named("g");
          }
        });
    assertThrows("Should complain that repeated group is empty",
        IllegalStateException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.repeatedGroup().named("g");
          }
        });
  }

  @Test
  @Ignore(value="Enforcing this breaks tests in parquet-thrift")
  public void testEmptyMessage() {
    assertThrows("Should complain that message is empty",
        IllegalStateException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.buildMessage().named("m");
          }
        });
  }

  @Test(expected=IllegalArgumentException.class)
  public void testFixedWithoutLength() {
    Types.required(FIXED_LEN_BYTE_ARRAY).named("fixed");
  }

  @Test
  public void testFixedWithLength() {
    PrimitiveType expected = new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, 7, "fixed");
    PrimitiveType fixed = Types.required(FIXED_LEN_BYTE_ARRAY).length(7).named("fixed");
    Assert.assertEquals(expected, fixed);
  }

  @Test
  public void testFixedLengthEquals() {
    Type f4 = Types.required(FIXED_LEN_BYTE_ARRAY).length(4).named("f4");
    Type f8 = Types.required(FIXED_LEN_BYTE_ARRAY).length(8).named("f8");
    Assert.assertFalse("Types with different lengths should not be equal",
        f4.equals(f8));
  }

  @Test
  public void testDecimalAnnotation() {
    // int32 primitive type
    MessageType expected = new MessageType("DecimalMessage",
        new PrimitiveType(REQUIRED, INT32, 0, "aDecimal",
            OriginalType.DECIMAL, new OriginalTypeMeta(9, 2)));
    MessageType builderType = Types.buildMessage()
        .required(INT32)
            .as(OriginalType.DECIMAL).precision(9).scale(2)
            .named("aDecimal")
        .named("DecimalMessage");
    Assert.assertEquals(expected, builderType);
    // int64 primitive type
    expected = new MessageType("DecimalMessage",
        new PrimitiveType(REQUIRED, INT64, 0, "aDecimal",
            OriginalType.DECIMAL, new OriginalTypeMeta(18, 2)));
    builderType = Types.buildMessage()
        .required(INT64)
            .as(OriginalType.DECIMAL).precision(18).scale(2)
            .named("aDecimal")
        .named("DecimalMessage");
    Assert.assertEquals(expected, builderType);
    // binary primitive type
    expected = new MessageType("DecimalMessage",
        new PrimitiveType(REQUIRED, BINARY, 0, "aDecimal",
            OriginalType.DECIMAL, new OriginalTypeMeta(9, 2)));
    builderType = Types.buildMessage()
        .required(BINARY).as(OriginalType.DECIMAL).precision(9).scale(2)
            .named("aDecimal")
        .named("DecimalMessage");
    Assert.assertEquals(expected, builderType);
    // fixed primitive type
    expected = new MessageType("DecimalMessage",
        new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, 4, "aDecimal",
            OriginalType.DECIMAL, new OriginalTypeMeta(9, 2)));
    builderType = Types.buildMessage()
        .required(FIXED_LEN_BYTE_ARRAY).length(4)
            .as(OriginalType.DECIMAL).precision(9).scale(2)
            .named("aDecimal")
        .named("DecimalMessage");
    Assert.assertEquals(expected, builderType);
  }

  @Test
  public void testDecimalAnnotationMissingScale() {
    MessageType expected = new MessageType("DecimalMessage",
        new PrimitiveType(REQUIRED, INT32, 0, "aDecimal",
            OriginalType.DECIMAL, new OriginalTypeMeta(9, 0)));
    MessageType builderType = Types.buildMessage()
        .required(INT32)
            .as(OriginalType.DECIMAL).precision(9)
            .named("aDecimal")
        .named("DecimalMessage");
    Assert.assertEquals(expected, builderType);

    expected = new MessageType("DecimalMessage",
        new PrimitiveType(REQUIRED, INT64, 0, "aDecimal",
            OriginalType.DECIMAL, new OriginalTypeMeta(9, 0)));
    builderType = Types.buildMessage()
        .required(INT64)
            .as(OriginalType.DECIMAL).precision(9)
            .named("aDecimal")
        .named("DecimalMessage");
    Assert.assertEquals(expected, builderType);

    expected = new MessageType("DecimalMessage",
        new PrimitiveType(REQUIRED, BINARY, 0, "aDecimal",
            OriginalType.DECIMAL, new OriginalTypeMeta(9, 0)));
    builderType = Types.buildMessage()
        .required(BINARY).as(OriginalType.DECIMAL).precision(9)
            .named("aDecimal")
        .named("DecimalMessage");
    Assert.assertEquals(expected, builderType);

    expected = new MessageType("DecimalMessage",
        new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, 7, "aDecimal",
            OriginalType.DECIMAL, new OriginalTypeMeta(9, 0)));
    builderType = Types.buildMessage()
        .required(FIXED_LEN_BYTE_ARRAY).length(7)
            .as(OriginalType.DECIMAL).precision(9)
            .named("aDecimal")
        .named("DecimalMessage");
    Assert.assertEquals(expected, builderType);
  }

  @Test
  public void testDecimalAnnotationMissingPrecision() {
    assertThrows("Should reject decimal annotation without precision",
        IllegalArgumentException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.buildMessage()
                .required(INT32).as(OriginalType.DECIMAL).scale(2)
                    .named("aDecimal")
                .named("DecimalMessage");
          }
        });
    assertThrows("Should reject decimal annotation without precision",
        IllegalArgumentException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.buildMessage()
                .required(INT64).as(OriginalType.DECIMAL).scale(2)
                    .named("aDecimal")
                .named("DecimalMessage");
          }
        });
    assertThrows("Should reject decimal annotation without precision",
        IllegalArgumentException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.buildMessage()
                .required(BINARY).as(OriginalType.DECIMAL).scale(2)
                    .named("aDecimal")
                .named("DecimalMessage");
          }
        });
    assertThrows("Should reject decimal annotation without precision",
        IllegalArgumentException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.buildMessage()
                .required(FIXED_LEN_BYTE_ARRAY).length(7)
                    .as(OriginalType.DECIMAL).scale(2)
                    .named("aDecimal")
                .named("DecimalMessage");
          }
        });
  }

  @Test
  public void testDecimalAnnotationPrecisionScaleBound() {
    assertThrows("Should reject scale greater than precision",
        IllegalArgumentException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.buildMessage()
                .required(INT32).as(OriginalType.DECIMAL).precision(3).scale(4)
                    .named("aDecimal")
                .named("DecimalMessage");
          }
        });
    assertThrows("Should reject scale greater than precision",
        IllegalArgumentException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.buildMessage()
                .required(INT64).as(OriginalType.DECIMAL).precision(3).scale(4)
                    .named("aDecimal")
                .named("DecimalMessage");
          }
        });
    assertThrows("Should reject scale greater than precision",
        IllegalArgumentException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.buildMessage()
                .required(BINARY).as(OriginalType.DECIMAL).precision(3).scale(4)
                    .named("aDecimal")
                .named("DecimalMessage");
          }
        });
    assertThrows("Should reject scale greater than precision",
        IllegalArgumentException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.buildMessage()
                .required(FIXED_LEN_BYTE_ARRAY).length(7)
                    .as(OriginalType.DECIMAL).precision(3).scale(4)
                    .named("aDecimal")
                .named("DecimalMessage");
          }
        });
  }

  @Test
  public void testDecimalAnnotationLengthCheck() {
    // maximum precision for 4 bytes is 9
    assertThrows("should reject precision 10 with length 4",
        IllegalStateException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.required(FIXED_LEN_BYTE_ARRAY).length(4)
                .as(OriginalType.DECIMAL).precision(10).scale(2)
                .named("aDecimal");
          }
        });
    assertThrows("should reject precision 10 with length 4",
        IllegalStateException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.required(INT32)
                .as(OriginalType.DECIMAL).precision(10).scale(2)
                .named("aDecimal");
          }
        });
    // maximum precision for 8 bytes is 19
    assertThrows("should reject precision 19 with length 8",
        IllegalStateException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.required(FIXED_LEN_BYTE_ARRAY).length(8)
                .as(OriginalType.DECIMAL).precision(19).scale(4)
                .named("aDecimal");
          }
        });
    assertThrows("should reject precision 19 with length 8",
        IllegalStateException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.required(INT64).length(8)
                .as(OriginalType.DECIMAL).precision(19).scale(4)
                .named("aDecimal");
          }
        });
  }

  @Test
  public void testDECIMALAnnotationRejectsUnsupportedTypes() {
    PrimitiveTypeName[] unsupported = new PrimitiveTypeName[]{
        BOOLEAN, INT96, DOUBLE, FLOAT
    };
    for (final PrimitiveTypeName type : unsupported) {
      assertThrows("Should reject non-binary type: " + type,
          IllegalStateException.class, new Callable<Type>() {
            @Override
            public Type call() throws Exception {
              return Types.required(type)
                  .as(OriginalType.DECIMAL).precision(9).scale(2)
                  .named("d");
            }
          });
    }
  }

  @Test
  public void testUTF8Annotation() {
    PrimitiveType expected = new PrimitiveType(REQUIRED, BINARY, "string", OriginalType.UTF8);
    PrimitiveType string = Types.required(BINARY).as(OriginalType.UTF8).named("string");
    Assert.assertEquals(expected, string);
  }

  @Test
  public void testUTF8AnnotationRejectsNonBinary() {
    PrimitiveTypeName[] nonBinary = new PrimitiveTypeName[]{
      BOOLEAN, INT32, INT64, INT96, DOUBLE, FLOAT
    };
    for (final PrimitiveTypeName type : nonBinary) {
      assertThrows("Should reject non-binary type: " + type,
          IllegalStateException.class, new Callable<Type>() {
            @Override
            public Type call() throws Exception {
              return Types.required(type).as(OriginalType.UTF8).named("string");
            }
          });
    }
    assertThrows("Should reject non-binary type: FIXED_LEN_BYTE_ARRAY",
        IllegalStateException.class, new Callable<Type>() {
          @Override
          public Type call() throws Exception {
            return Types.required(FIXED_LEN_BYTE_ARRAY).length(1)
                .as(OriginalType.UTF8).named("string");
          }
        });
  }


  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   * @param message A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param callable A Callable that is expected to throw the exception
   */
  public static void assertThrows(
      String message, Class<? extends Exception> expected, Callable callable) {
    try {
      callable.call();
      Assert.fail("No exception was thrown (" + message + "), expected: " +
          expected.getName());
    } catch (Exception actual) {
      Assert.assertEquals(message, expected, actual.getClass());
    }
  }
}
