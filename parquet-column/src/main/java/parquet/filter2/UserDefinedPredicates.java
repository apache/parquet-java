package parquet.filter2;

import parquet.io.api.Binary;

/**
 * User defined (custom) filter predicate for dropping records or groups of records.
 */
public final class UserDefinedPredicates {
  private UserDefinedPredicates() { }

  public static abstract class UserDefinedPredicate<T> {
    // package private so that this can't be subclassed directly outside of this package
    UserDefinedPredicate() { }
  }

  public static abstract class IntUserDefinedPredicate extends UserDefinedPredicate<Integer> {
    public IntUserDefinedPredicate() { }

    /**
     * Return true to keep the record with this value, false to drop it.
     */
    public abstract boolean keep(int value);

    /**
     * Given the min and max values of a group of records,
     * Return true to drop all the records in this group, false to keep them for further
     * inspection. Returning false here will cause the records to be loaded and each value
     * will be passed to {@link #keep} to make the final decision.
     */
    public abstract boolean canDrop(int min, int max);

    /**
     * Same as {@link #canDrop} except this method describes the logical inverse
     * behavior of this predicate. If this predicate is passed to the not() operator, then
     * {@link #inverseCanDrop} will be called instead of {@link #canDrop}
     *
     * It may be valid to simply return !canDrop(min, max) but that is not always the case.
     */
    public abstract boolean inverseCanDrop(int min, int max);
  }

  public static abstract class LongUserDefinedPredicate extends UserDefinedPredicate<Long> {
    public LongUserDefinedPredicate() { }

    /**
     * Return true to keep the record with this value, false to drop it.
     */
    public abstract boolean keep(long value);

    /**
     * Given the min and max values of a group of records,
     * Return true to drop all the records in this group, false to keep them for further
     * inspection. Returning false here will cause the records to be loaded and each value
     * will be passed to {@link #keep} to make the final decision.
     */
    public abstract boolean canDrop(long min, long max);

    /**
     * Same as {@link #canDrop} except this method describes the logical inverse
     * behavior of this predicate. If this predicate is passed to the not() operator, then
     * {@link #inverseCanDrop} will be called instead of {@link #canDrop}
     *
     * It may be valid to simply return !canDrop(min, max) but that is not always the case.
     */
    public abstract boolean inverseCanDrop(long min, long max);
  }

  public static abstract class FloatUserDefinedPredicate extends UserDefinedPredicate<Float> {
    public FloatUserDefinedPredicate() { }

    /**
     * Return true to keep the record with this value, false to drop it.
     */
    public abstract boolean keep(float value);

    /**
     * Given the min and max values of a group of records,
     * Return true to drop all the records in this group, false to keep them for further
     * inspection. Returning false here will cause the records to be loaded and each value
     * will be passed to {@link #keep)} to make the final decision.
     */
    public abstract boolean canDrop(float min, float max);

    /**
     * Same as {@link #canDrop} except this method describes the logical inverse
     * behavior of this predicate. If this predicate is passed to the not() operator, then
     * {@link #inverseCanDrop} will be called instead of {@link #canDrop}
     *
     * It may be valid to simply return !canDrop(min, max) but that is not always the case.
     */
    public abstract boolean inverseCanDrop(float min, float max);
  }

  public static abstract class DoubleUserDefinedPredicate extends UserDefinedPredicate<Double> {
    public DoubleUserDefinedPredicate() { }

    /**
     * Return true to keep the record with this value, false to drop it.
     */
    public abstract boolean keep(double value);

    /**
     * Given the min and max values of a group of records,
     * Return true to drop all the records in this group, false to keep them for further
     * inspection. Returning false here will cause the records to be loaded and each value
     * will be passed to {@link #keep} to make the final decision.
     */
    public abstract boolean canDrop(double min, double max);

    /**
     * Same as {@link #canDrop} except this method describes the logical inverse
     * behavior of this predicate. If this predicate is passed to the not() operator, then
     * {@link #inverseCanDrop} will be called instead of {@link #canDrop}
     *
     * It may be valid to simply return !canDrop(min, max) but that is not always the case.
     */
    public abstract boolean inverseCanDrop(double min, double max);
  }

  // Note: there's no BooleanUserDefinedPredicate because there's nothing you can do with a boolean
  // that you can't do with eq()

  public static abstract class BinaryUserDefinedPredicate extends UserDefinedPredicate<Binary> {
    public BinaryUserDefinedPredicate() { }

    /**
     * Return true to keep the record with this value, false to drop it.
     */
    public abstract boolean keep(Binary value);

    /**
     * Given the min and max values of a group of records,
     * Return true to drop all the records in this group, false to keep them for further
     * inspection. Returning false here will cause the records to be loaded and each value
     * will be passed to {@link #keep} to make the final decision.
     */
    public abstract boolean canDrop(Binary min, Binary max);

    /**
     * Same as {@link #canDrop} except this method describes the logical inverse
     * behavior of this predicate. If this predicate is passed to the not() operator, then
     * {@link #inverseCanDrop} will be called instead of {@link #canDrop}
     *
     * It may be valid to simply return !canDrop(min, max) but that is not always the case.
     */
    public abstract boolean inverseCanDrop(Binary min, Binary max);
  }

  public static abstract class StringUserDefinedPredicate extends UserDefinedPredicate<String> {
    public StringUserDefinedPredicate() { }

    /**
     * Return true to keep the record with this value, false to drop it.
     */
    public abstract boolean keep(String value);

    /**
     * Given the min and max values of a group of records,
     * Return true to drop all the records in this group, false to keep them for further
     * inspection. Returning false here will cause the records to be loaded and each value
     * will be passed to {@link #keep} to make the final decision.
     */
    public abstract boolean canDrop(String min, String max);

    /**
     * Same as {@link #canDrop} except this method describes the logical inverse
     * behavior of this predicate. If this predicate is passed to the not() operator, then
     * {@link #inverseCanDrop} will be called instead of {@link #canDrop}
     *
     * It may be valid to simply return !canDrop(min, max) but that is not always the case.
     */
    public abstract boolean inverseCanDrop(String min, String max);
  }

}
