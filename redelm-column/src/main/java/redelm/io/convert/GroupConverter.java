package redelm.io.convert;

/**
 * Represent a tree of converters
 * that materializes tuples
 *
 * @author Julien Le Dem
 *
 */
public abstract class GroupConverter {

  /**
   * called at initialization based on schema
   * must consistently return the same object
   * @param fieldIndex index of a group field in this group
   * @return the corresponding converter
   */
  abstract public GroupConverter getGroupConverter(int fieldIndex);

  /**
   * called at initialization based on schema
   * must consistently return the same object
   * @param fieldIndex index of a primitive field in this group
   * @return the corresponding converter
   */
  abstract public PrimitiveConverter getPrimitiveConverter(int fieldIndex);


  /** runtime calls  **/

  /** called at the beginning of the group managed by this converter */
  public abstract void start();

  /**
   * call at the end of the group
   */
  public abstract void end();

}
