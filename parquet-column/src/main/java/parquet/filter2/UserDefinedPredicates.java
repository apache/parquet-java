package parquet.filter2;

public final class UserDefinedPredicates {
  private UserDefinedPredicates() { }

  private static final String FILTER_MIN_MAX_NOT_IMPLEMENTED =
      "You returned true from supportsFilterByMinMax() but forgot to override filterByMinMax()!";

  public static abstract class UserDefinedPredicate<T> {
    // package private so that this can't be sublcassed directly outside of this package
    UserDefinedPredicate() { }

    public boolean supportsFilterByMinMax() { return  false; }
  }

  public static abstract class IntUserDefinedPredicate extends UserDefinedPredicate<Integer> {
    public IntUserDefinedPredicate() { }
    public abstract boolean filterByValue(int value);
    public boolean filterByMinMax(int min, int max) { throw new UnsupportedOperationException(FILTER_MIN_MAX_NOT_IMPLEMENTED); }
  }

  public static abstract class LongUserDefinedPredicate extends UserDefinedPredicate<Long> {
    public LongUserDefinedPredicate() { }
    public abstract boolean filterByValue(long value);
    public boolean filterByMinMax(long min, long max) { throw new UnsupportedOperationException(FILTER_MIN_MAX_NOT_IMPLEMENTED); }
  }

  public static abstract class FloatUserDefinedPredicate extends UserDefinedPredicate<Float> {
    public FloatUserDefinedPredicate() { }
    public abstract boolean filterByValue(float value);
    public boolean filterByMinMax(float min, float max) { throw new UnsupportedOperationException(FILTER_MIN_MAX_NOT_IMPLEMENTED); }
  }

  public static abstract class DoubleUserDefinedPredicate extends UserDefinedPredicate<Double> {
    public DoubleUserDefinedPredicate() { }
    public abstract boolean filterByValue(double value);
    public boolean filterByMinMax(double min, double max) { throw new UnsupportedOperationException(FILTER_MIN_MAX_NOT_IMPLEMENTED); }
  }

  // Note: there's no BooleanUserDefinedPredicate because there's nothing you can do with a boolean
  // that you can't do with eq()

  public static abstract class BinaryUserDefinedPredicate extends UserDefinedPredicate<byte[]> {
    public BinaryUserDefinedPredicate() { }
    public abstract boolean filterByValue(byte[] value);
    public boolean filterByMinMax(byte[] min, byte[] max) { throw new UnsupportedOperationException(FILTER_MIN_MAX_NOT_IMPLEMENTED); }
  }

  public static abstract class StringUserDefinedPredicate extends UserDefinedPredicate<String> {
    public StringUserDefinedPredicate() { }
    public abstract boolean filterByValue(String value);
    public boolean filterByMinMax(String min, String max) { throw new UnsupportedOperationException(FILTER_MIN_MAX_NOT_IMPLEMENTED); }
  }

}
