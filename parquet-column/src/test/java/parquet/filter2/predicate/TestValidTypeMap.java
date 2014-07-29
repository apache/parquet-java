package parquet.filter2.predicate;

import org.junit.Test;

import parquet.common.schema.ColumnPath;
import parquet.filter2.predicate.Operators.BinaryColumn;
import parquet.filter2.predicate.Operators.BooleanColumn;
import parquet.filter2.predicate.Operators.Column;
import parquet.filter2.predicate.Operators.DoubleColumn;
import parquet.filter2.predicate.Operators.FloatColumn;
import parquet.filter2.predicate.Operators.IntColumn;
import parquet.filter2.predicate.Operators.LongColumn;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static parquet.filter2.predicate.FilterApi.binaryColumn;
import static parquet.filter2.predicate.FilterApi.booleanColumn;
import static parquet.filter2.predicate.FilterApi.doubleColumn;
import static parquet.filter2.predicate.FilterApi.floatColumn;
import static parquet.filter2.predicate.FilterApi.intColumn;
import static parquet.filter2.predicate.FilterApi.longColumn;
import static parquet.filter2.predicate.ValidTypeMap.assertTypeValid;

public class TestValidTypeMap {
  public static IntColumn intColumn = intColumn("int.column");
  public static LongColumn longColumn = longColumn("long.column");
  public static FloatColumn floatColumn = floatColumn("float.column");
  public static DoubleColumn doubleColumn = doubleColumn("double.column");
  public static BooleanColumn booleanColumn = booleanColumn("boolean.column");
  public static BinaryColumn binaryColumn = binaryColumn("binary.column");

  private static class InvalidColumnType implements Comparable<InvalidColumnType> {
    @Override
    public int compareTo(InvalidColumnType o) {
      return 0;
    }
  }

  public static Column<InvalidColumnType> invalidColumn =
      new Column<InvalidColumnType>(ColumnPath.get("invalid.column"), InvalidColumnType.class) { };

  @Test
  public void testValidTypes() {
    assertTypeValid(intColumn, PrimitiveTypeName.INT32, null);
    assertTypeValid(longColumn, PrimitiveTypeName.INT64, null);
    assertTypeValid(floatColumn, PrimitiveTypeName.FLOAT, null);
    assertTypeValid(doubleColumn, PrimitiveTypeName.DOUBLE, null);
    assertTypeValid(booleanColumn, PrimitiveTypeName.BOOLEAN, null);
    assertTypeValid(binaryColumn, PrimitiveTypeName.BINARY, null);
    assertTypeValid(binaryColumn, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, null);
    assertTypeValid(binaryColumn, PrimitiveTypeName.BINARY, OriginalType.UTF8);
    assertTypeValid(binaryColumn, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, OriginalType.UTF8);
  }

  @Test
  public void testMismatchedTypes() {
    try {
      assertTypeValid(intColumn, PrimitiveTypeName.DOUBLE, null);
      fail("This should throw!");
    } catch (IllegalArgumentException e) {
      assertEquals("FilterPredicate column: int.column's declared type (java.lang.Integer) does not match the "
          + "schema found in file metadata. Column int.column is of type: "
          + "FullTypeDescriptor(PrimitiveType: DOUBLE, OriginalType: null)\n"
          + "Valid types for this column are: [class java.lang.Double]", e.getMessage());
    }
  }

  @Test
  public void testUnsupportedType() {
    try {
      assertTypeValid(invalidColumn, PrimitiveTypeName.INT32, null);
      fail("This should throw!");
    } catch (IllegalArgumentException e) {
      assertEquals("Column invalid.column was declared as type: "
          + "parquet.filter2.predicate.TestValidTypeMap$InvalidColumnType which is not supported "
          + "in FilterPredicates. Supported types for this column are: [class java.lang.Integer]", e.getMessage());
    }

    try {
      assertTypeValid(invalidColumn, PrimitiveTypeName.INT32, OriginalType.UTF8);
      fail("This should throw!");
    } catch (IllegalArgumentException e) {
      assertEquals("Column invalid.column was declared as type: "
          + "parquet.filter2.predicate.TestValidTypeMap$InvalidColumnType which is not supported "
          + "in FilterPredicates. There are no supported types for columns of FullTypeDescriptor(PrimitiveType: INT32, OriginalType: UTF8)",
          e.getMessage());
    }

  }

}
