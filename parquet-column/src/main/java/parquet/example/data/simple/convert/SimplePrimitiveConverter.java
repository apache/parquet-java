package parquet.example.data.simple.convert;

import parquet.io.convert.PrimitiveConverter;

class SimplePrimitiveConverter extends PrimitiveConverter {

  private final SimpleGroupConverter parent;
  private final int index;

  SimplePrimitiveConverter(SimpleGroupConverter parent,
      int index) {
        this.parent = parent;
        this.index = index;
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.convert.PrimitiveConverter#addBinary(byte[])
   */
  @Override
  public void addBinary(byte[] value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.convert.PrimitiveConverter#addBoolean(boolean)
   */
  @Override
  public void addBoolean(boolean value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.convert.PrimitiveConverter#addDouble(double)
   */
  @Override
  public void addDouble(double value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.convert.PrimitiveConverter#addFloat(float)
   */
  @Override
  public void addFloat(float value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.convert.PrimitiveConverter#addInt(int)
   */
  @Override
  public void addInt(int value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.convert.PrimitiveConverter#addLong(long)
   */
  @Override
  public void addLong(long value) {
    parent.getCurrentRecord().add(index, value);
  }

}
