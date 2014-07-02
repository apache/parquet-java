package parquet.filter2;

import org.junit.Test;

import parquet.filter2.FilterPredicateOperators.Column;
import parquet.io.api.Binary;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static parquet.filter2.Filter.binaryColumn;
import static parquet.filter2.Filter.booleanColumn;
import static parquet.filter2.Filter.doubleColumn;
import static parquet.filter2.Filter.floatColumn;
import static parquet.filter2.Filter.intColumn;
import static parquet.filter2.Filter.longColumn;
import static parquet.filter2.ValidTypeMap.assertTypeValid;

public class TestValidTypeMap {
  public static Column<Integer> intColumn = intColumn("int.column");
  public static Column<Long> longColumn = longColumn("long.column");
  public static Column<Float> floatColumn = floatColumn("float.column");
  public static Column<Double> doubleColumn = doubleColumn("double.column");
  public static Column<Boolean> booleanColumn = booleanColumn("boolean.column");
  public static Column<Binary> binaryColumn = binaryColumn("binary.column");

  private static class InvalidColumnType implements Comparable<InvalidColumnType> {
    @Override
    public int compareTo(InvalidColumnType o) {
      return 0;
    }
  }

  public static Column<InvalidColumnType> invalidColumn =
      new Column<InvalidColumnType>("invalid.column", InvalidColumnType.class);

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
          + "parquet.filter2.TestValidTypeMap$InvalidColumnType which is not supported "
          + "in FilterPredicates. Supported types for this column are: [class java.lang.Integer]", e.getMessage());
    }

    try {
      assertTypeValid(invalidColumn, PrimitiveTypeName.INT32, OriginalType.UTF8);
      fail("This should throw!");
    } catch (IllegalArgumentException e) {
      assertEquals("Column invalid.column was declared as type: "
          + "parquet.filter2.TestValidTypeMap$InvalidColumnType which is not supported "
          + "in FilterPredicates. There are no supported types for columns of FullTypeDescriptor(PrimitiveType: INT32, OriginalType: UTF8)",
          e.getMessage());
    }

  }

}
