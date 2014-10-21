package parquet.column.values;

public interface RequiresFallback {

  boolean shouldFallBack();

  boolean isCompressionSatisfying(long rawSize, long encodedSize);

  void fallBackAllValuesTo(ValuesWriter writer);

}
