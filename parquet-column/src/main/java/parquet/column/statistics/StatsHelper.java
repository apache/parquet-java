package parquet.column.statistics;

import parquet.column.UnknownColumnTypeException;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

public class StatsHelper {

  public static Statistics getStatsBasedOnType(PrimitiveTypeName type) {
    switch(type) {
    case INT32:
      return new IntStatistics();
    case INT64:
      return new LongStatistics();
    case FLOAT:
      return new FloatStatistics();
    case DOUBLE:
      return new DoubleStatistics();
    case BOOLEAN:
      return new BooleanStatistics();
    case BINARY:
      return new BinaryStatistics();
    case INT96:
      return new BinaryStatistics();
    case FIXED_LEN_BYTE_ARRAY:
      return new BinaryStatistics();
    default:
      throw new UnknownColumnTypeException(type);
    }
  }
}
