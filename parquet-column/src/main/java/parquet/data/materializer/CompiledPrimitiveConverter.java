package parquet.data.materializer;

import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.PrimitiveType;

public abstract class CompiledPrimitiveConverter extends PrimitiveConverter {

  abstract public PrimitiveType getType();

  private void error() {
    throw new UnsupportedOperationException(getClass().getName() + " type:" + getType());
  }

  @Override
  public void addValueFromDictionary(int dictionaryId) {
    error();
  }

  @Override
  public void addBinary(Binary value) {
    error();
  }

  @Override
  public void addBoolean(boolean value) {
    error();
  }

  @Override
  public void addDouble(double value) {
    error();
  }

  @Override
  public void addFloat(float value) {
    error();
  }

  @Override
  public void addInt(int value) {
    error();
  }

  @Override
  public void addLong(long value) {
    error();
  }

}
