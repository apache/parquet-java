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
   *
   * It is safe to always return false here, if you simply want to visit each record via the {@link #keep} method,
   * though it is much more efficient to drop entire chunks of records here if you can.
   */
  public abstract boolean canDrop(T min, T max);

  /**
   * Same as {@link #canDrop} except this method describes the logical inverse
   * behavior of this predicate. If this predicate is passed to the not() operator, then
   * {@link #inverseCanDrop} will be called instead of {@link #canDrop}
   *
   * It is safe to always return false here, if you simply want to visit each record via the {@link #keep} method,
   * though it is much more efficient to drop entire chunks of records here if you can.
   *
   * It may be valid to simply return !canDrop(min, max) but that is not always the case.
   * To illustrate, look at this re-implementation of a UDP that checks for values greater than 7:
   *
   * {@code 
   * 
   * // This is just an example, you should use the built in {@link FilterApi#gt} operator instead of
   * // implementing your own like this.
   *  
   * public class IntGreaterThan7UDP extends UserDefinedPredicate<Integer> {
   *   @Override
   *   public boolean keep(Integer value) {
   *     // here we just check if the value is greater than 7.
   *     // here, parquet knows that if the predicate not(columnX, IntGreaterThan7UDP) is being evaluated,
   *     // it is safe to simply use !IntEquals7UDP.keep(value)
   *     return value > 7;
   *   }
   * 
   *   @Override
   *   public boolean canDrop(Integer min, Integer max) {
   *     // here we drop a group of records if they are all less than or equal to 7,
   *     // (there can't possibly be any values greater than 7 in this group of records)
   *     return max <= 7;
   *   }
   * 
   *   @Override
   *   public boolean inverseCanDrop(Integer min, Integer max) {
   *     // here the predicate not(columnX, IntGreaterThan7UDP) is being evaluated, which means we want
   *     // to keep all records whose value is is not greater than 7, or, rephrased, whose value is less than or equal to 7.
   *     // notice what would happen if parquet just tried to evaluate !IntGreaterThan7UDP.canDrop():
   *     // !IntGreaterThan7UDP.canDrop(min, max) == !(max <= 7) == (max > 7)
   *     // it would drop the following group of records: [100, 1, 2, 3], even though this group of records contains values
   *     // less than than or equal to 7.
   * 
   *     // what we actually want to do is drop groups of records where the *min* is greater than 7, (not the max)
   *     // for example: the group of records: [100, 8, 9, 10] has a min of 8, so there's no way there are going
   *     // to be records with a value
   *     // less than or equal to 7 in this group.
   *     return min > 7;
   *   }
   * }
   * }
   */
  public abstract boolean inverseCanDrop(T min, T max);
}