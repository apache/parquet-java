package parquet.filter2;

import parquet.column.ColumnReader;
import parquet.io.api.Binary;

public abstract class GenericColumnReader<T extends Comparable<T>> {
  protected final ColumnReader reader;

  public GenericColumnReader(ColumnReader reader) {
    this.reader = reader;
  }

  public  abstract T getCurrentValue();

  public static class IntColumnReader extends GenericColumnReader<Integer> {

    public IntColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Integer getCurrentValue() {
      return reader.getInteger();
    }
  }

  public static class LongColumnReader extends GenericColumnReader<Long> {

    public LongColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Long getCurrentValue() {
      return reader.getLong();
    }
  }

  public static class DoubleColumnReader extends GenericColumnReader<Double> {

    public DoubleColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Double getCurrentValue() {
      return reader.getDouble();
    }
  }

  public static class FloatColumnReader extends GenericColumnReader<Float> {

    public FloatColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Float getCurrentValue() {
      return reader.getFloat();
    }
  }

  public static class BooleanColumnReader extends GenericColumnReader<Boolean> {

    public BooleanColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Boolean getCurrentValue() {
      return reader.getBoolean();
    }
  }

  public static class BinaryColumnReader extends GenericColumnReader<Binary> {

    public BinaryColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Binary getCurrentValue() {
      return reader.getBinary();
    }
  }

}
