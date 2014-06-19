package parquet.filter2;

import java.util.Set;

public final class UserDefinedPredicates {
  private UserDefinedPredicates() { }

  public static abstract class UserDefinedPredicate<T> {
    // package private so that this can't be sublcassed directly outside of this package
    UserDefinedPredicate() { }
  }

  public static abstract class IntUserDefinedPredicate extends UserDefinedPredicate<Integer> {
    public IntUserDefinedPredicate() { }
    public abstract boolean filterByValue(int value);
    public boolean filterByMinMax(int min, int max) { return  true; }
  }

  public static abstract class LongUserDefinedPredicate extends UserDefinedPredicate<Long> {
    public LongUserDefinedPredicate() { }
    public abstract boolean filterByValue(long value);
    public boolean filterByMinMax(long min, long max) { return  true; }
  }

  public static abstract class FloatUserDefinedPredicate extends UserDefinedPredicate<Float> {
    public FloatUserDefinedPredicate() { }
    public abstract boolean filterByValue(float value);
    public boolean filterByMinMax(float min, float max) { return  true; }
  }

  public static abstract class DoubleUserDefinedPredicate extends UserDefinedPredicate<Double> {
    public DoubleUserDefinedPredicate() { }
    public abstract boolean filterByValue(double value);
    public boolean filterByMinMax(double min, double max) { return  true; }
  }

  // Note: there's no BooleanUserDefinedPredicate because there's nothing you can do with a boolean
  // that you can't do with eq()

  public static abstract class BinaryUserDefinedPredicate extends UserDefinedPredicate<byte[]> {
    public BinaryUserDefinedPredicate() { }
    public abstract boolean filterByValue(byte[] value);
    public boolean filterByMinMax(byte[] min, byte[] max) { return  true; }
    public boolean filterByUniqueValues(Set<byte[]> uniqueValues) { return true; }
  }

  public static abstract class StringUserDefinedPredicate extends UserDefinedPredicate<String> {
    public StringUserDefinedPredicate() { }
    public abstract boolean filterByValue(String value);
    public boolean filterByMinMax(String min, String max) { return  true; }
    public boolean filterByUniqueValues(Set<String> uniqueValues) { return true; }
  }

}
