package parquet.io.convert;

/**
 * converter for leaves of the schema
 *
 * @author Julien Le Dem
 *
 */
abstract public class PrimitiveConverter extends Converter {

  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addBinary(byte[] value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addBoolean(boolean value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addDouble(double value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addFloat(float value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addInt(int value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param fieldIndex index of the field
   * @param value value to set
   */
  public void addLong(long value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public boolean isPrimitive() {
    return true;
  }

  @Override
  public PrimitiveConverter asPrimitiveConverter() {
    return this;
  }

}
