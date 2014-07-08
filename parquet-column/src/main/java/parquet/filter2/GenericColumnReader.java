package parquet.filter2;

import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.io.api.Binary;

/**
 * Provides a generic "view" to a ColumnReader
 * TODO(alexlevenson): make ColumnReader generic in the first place
 */
public abstract class GenericColumnReader<T extends Comparable<T>> {
  protected final ColumnReader reader;
  private final ColumnDescriptor descriptor;

  public GenericColumnReader(ColumnReader reader) {
    this.reader = reader;
    this.descriptor = reader.getDescriptor();
  }

  public boolean isCurrentValueNull() {
    return reader.getCurrentDefinitionLevel() < descriptor.getMaxDefinitionLevel();
  }

  public T getValueOrNull() {
    if (isCurrentValueNull()) {
      return null;
    }
    return getValue();
  }

  public abstract T getValue();

  public static class IntColumnReader extends GenericColumnReader<Integer> {

    public IntColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Integer getValue() {
      return reader.getInteger();
    }
  }

  public static class LongColumnReader extends GenericColumnReader<Long> {

    public LongColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Long getValue() {
      return reader.getLong();
    }
  }

  public static class DoubleColumnReader extends GenericColumnReader<Double> {

    public DoubleColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Double getValue() {
      return reader.getDouble();
    }
  }

  public static class FloatColumnReader extends GenericColumnReader<Float> {

    public FloatColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Float getValue() {
      return reader.getFloat();
    }
  }

  public static class BooleanColumnReader extends GenericColumnReader<Boolean> {

    public BooleanColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Boolean getValue() {
      return reader.getBoolean();
    }
  }

  public static class BinaryColumnReader extends GenericColumnReader<Binary> {

    public BinaryColumnReader(ColumnReader reader) {
      super(reader);
    }

    @Override
    public Binary getValue() {
      return reader.getBinary();
    }
  }

}
