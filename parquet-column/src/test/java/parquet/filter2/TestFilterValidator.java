package parquet.filter2;

import org.junit.Test;

import parquet.filter2.FilterPredicateOperators.Column;
import parquet.io.api.Binary;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static parquet.filter2.Filter.and;
import static parquet.filter2.Filter.binaryColumn;
import static parquet.filter2.Filter.eq;
import static parquet.filter2.Filter.gt;
import static parquet.filter2.Filter.intColumn;
import static parquet.filter2.Filter.longColumn;
import static parquet.filter2.Filter.ltEq;
import static parquet.filter2.Filter.not;
import static parquet.filter2.Filter.notEq;
import static parquet.filter2.Filter.or;
import static parquet.filter2.Filter.userDefined;
import static parquet.filter2.FilterPredicateTypeValidator.validate;

public class TestFilterValidator {
  private static final Column<Binary> stringC = binaryColumn("c");
  private static final Column<Long> longBar = longColumn("x.bar");
  private static final Column<Integer> intBar = intColumn("x.bar");

  private static final String schemaString =
      "message Document {\n"
          + "  required int32 a;\n"
          + "  required binary b;\n"
          + "  required binary c (UTF8);\n"
          + "  required group x { required int32 bar; } "
          + "}\n";

  private static final MessageType schema = MessageTypeParser.parseMessageType(schemaString);

  private static final FilterPredicate complexValid =
      and(
          or(ltEq(stringC, Binary.fromString("foo")),
              and(
                  not(or(eq(intBar, 17), notEq(intBar, 17))),
                  userDefined(intBar, DummyUdp.class))),
          or(gt(stringC, Binary.fromString("bar")), notEq(stringC, Binary.fromString("baz"))));

  static class LongDummyUdp extends UserDefinedPredicate<Long> {
    @Override
    public boolean keep(Long value) {
      return false;
    }

    @Override
    public boolean canDrop(Long min, Long max) {
      return false;
    }

    @Override
    public boolean inverseCanDrop(Long min, Long max) {
      return false;
    }
  }

  private static final FilterPredicate complexWrongType =
      and(
          or(ltEq(stringC, Binary.fromString("foo")),
              and(
                  not(or(eq(longBar, 17L), notEq(longBar, 17L))),
                  userDefined(longBar, LongDummyUdp.class))),
          or(gt(stringC, Binary.fromString("bar")), notEq(stringC, Binary.fromString("baz"))));

  private static final FilterPredicate complexMixedType =
      and(
          or(ltEq(stringC, Binary.fromString("foo")),
              and(
                  not(or(eq(intBar, 17), notEq(longBar, 17L))),
                  userDefined(longBar, LongDummyUdp.class))),
          or(gt(stringC, Binary.fromString("bar")), notEq(stringC, Binary.fromString("baz"))));

  @Test
  public void testValidType() {
    validate(complexValid, schema);
  }

  @Test
  public void testFindsInvalidTypes() {
    try {
      validate(complexWrongType, schema);
      fail("this should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("FilterPredicate column: x.bar's declared type (java.lang.Long) does not match the schema found in file metadata. "
          + "Column x.bar is of type: FullTypeDescriptor(PrimitiveType: INT32, OriginalType: null)\n"
          + "Valid types for this column are: [class java.lang.Integer]", e.getMessage());
    }
  }

  @Test
  public void testTwiceDeclaredColumn() {
    validate(eq(stringC, Binary.fromString("larry")), schema);

    try {
      validate(complexMixedType, schema);
      fail("this should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("Column: x.bar was provided with different types in the same predicate. Found both: (class java.lang.Integer, class java.lang.Long)", e.getMessage());
    }

  }
}
