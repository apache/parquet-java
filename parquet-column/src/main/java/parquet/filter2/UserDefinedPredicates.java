package parquet.filter2;

import parquet.io.api.Binary;

public final class UserDefinedPredicates {
  private UserDefinedPredicates() { }

  private static final String FILTER_MIN_MAX_NOT_IMPLEMENTED =
      "You returned true from supportsFilterByMinMax() but forgot to override filterByMinMax()!";

  public static abstract class UserDefinedPredicate<T> {
    // package private so that this can't be sublcassed directly outside of this package
    UserDefinedPredicate() { }
  }

  public static abstract class IntUserDefinedPredicate extends UserDefinedPredicate<Integer> {
    public IntUserDefinedPredicate() { }
    public abstract boolean keep(int value);
    public abstract boolean canDrop(int min, int max);
    public abstract boolean inverseCanDrop(int min, int max);
  }

  public static abstract class LongUserDefinedPredicate extends UserDefinedPredicate<Long> {
    public LongUserDefinedPredicate() { }
    public abstract boolean keep(long value);
    public abstract boolean canDrop(long min, long max);
    public abstract boolean inverseCanDrop(long min, long max);
  }

  public static abstract class FloatUserDefinedPredicate extends UserDefinedPredicate<Float> {
    public FloatUserDefinedPredicate() { }
    public abstract boolean keep(float value);
    public abstract boolean canDrop(float min, float max);
    public abstract boolean inverseCanDrop(float min, float max);
  }

  public static abstract class DoubleUserDefinedPredicate extends UserDefinedPredicate<Double> {
    public DoubleUserDefinedPredicate() { }
    public abstract boolean keep(double value);
    public abstract boolean canDrop(double min, double max);
    public abstract boolean inverseCanDrop(double min, double max);
  }

  // Note: there's no BooleanUserDefinedPredicate because there's nothing you can do with a boolean
  // that you can't do with eq()

  public static abstract class BinaryUserDefinedPredicate extends UserDefinedPredicate<Binary> {
    public BinaryUserDefinedPredicate() { }
    public abstract boolean keep(Binary value);
    public abstract boolean canDrop(Binary min, Binary max);
    public abstract boolean inverseCanDrop(Binary min, Binary max);
  }

  public static abstract class StringUserDefinedPredicate extends UserDefinedPredicate<String> {
    public StringUserDefinedPredicate() { }
    public abstract boolean keep(String value);
    public abstract boolean canDrop(String min, String max);
    public abstract boolean inverseCanDrop(String min, String max);
  }

}
