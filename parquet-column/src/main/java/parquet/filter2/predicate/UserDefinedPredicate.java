package parquet.filter2.predicate;

/**
 * A UserDefinedPredicate decides whether a record should be kept or dropped, first by
 * inspecting meta data about a group of records to see if the entire group can be dropped,
 * then by inspecting actual values of a single column. These predicates can be combined into
 * a complex boolean expression via the {@link FilterApi}.
 *
 * @param <T> The type of the column this predicate is applied to.
 */
// TODO(alexlevenson): consider avoiding autoboxing and adding the specialized methods for each type
// TODO(alexlevenson): downside is that's fairly unwieldy for users
public abstract class UserDefinedPredicate<T extends Comparable<T>> {

  /**
   * A udp must have a default constructor.
   * The udp passed to {@link FilterApi} will not be serialized along with its state.
   * Only its class name will be recorded, it will be instantiated reflectively via the default
   * constructor.
   */
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