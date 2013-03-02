package parquet.io.convert;

/**
 * converter for group nodes
 *
 * @author Julien Le Dem
 *
 */
abstract public class GroupConverter extends Converter {

  /**
   * called at initialization based on schema
   * must consistently return the same object
   * @param fieldIndex index of a group field in this group
   * @return the corresponding converter
   */
  abstract public Converter getConverter(int fieldIndex);

  /** runtime calls  **/

  /** called at the beginning of the group managed by this converter */
  abstract public void start();

  /**
   * call at the end of the group
   */
  abstract public void end();


  @Override
  public boolean isPrimitive() {
    return false;
  }

  @Override
  public GroupConverter asGroupConverter() {
    return this;
  }
}
