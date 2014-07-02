package parquet.filter2;


public abstract class UserDefinedPredicate<T extends Comparable<T>> {
  public UserDefinedPredicate() { }

  /**
   * Return true to keep the record with this value, false to drop it.
   */
  public abstract boolean keep(T value);

  /**
   * Given the min and max values of a group of records,
   * Return true to drop all the records in this group, false to keep them for further
   * inspection. Returning false here will cause the records to be loaded and each value
   * will be passed to {@link #keep} to make the final decision.
   */
  public abstract boolean canDrop(T min, T max);

  /**
   * Same as {@link #canDrop} except this method describes the logical inverse
   * behavior of this predicate. If this predicate is passed to the not() operator, then
   * {@link #inverseCanDrop} will be called instead of {@link #canDrop}
   *
   * It may be valid to simply return !canDrop(min, max) but that is not always the case.
   */
  public abstract boolean inverseCanDrop(T min, T max);
}