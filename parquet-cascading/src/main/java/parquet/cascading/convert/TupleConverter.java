package parquet.cascading.convert;

import cascading.tuple.Tuple;

import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.pig.TupleConversionException;
import parquet.schema.GroupType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

public class TupleConverter extends GroupConverter {

  protected Tuple currentTuple;
  private final Converter[] converters;

  public TupleConverter(GroupType parquetSchema) {
    int schemaSize = parquetSchema.getFieldCount();

    this.converters = new Converter[schemaSize];
    for (int i = 0; i < schemaSize; i++) {
      Type type = parquetSchema.getType(i);
      converters[i] = newConverter(type, i);
    }
  }

  private Converter newConverter(Type type, int i) {
    if(!type.isPrimitive()) {
      throw new IllegalArgumentException("cascading can only build tuples from primitive types");
    } else {
      return new TuplePrimitiveConverter(this, i);
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  final public void start() {
    currentTuple = Tuple.size(converters.length);
  }

  @Override
  public void end() {
  }

  final public Tuple getCurrentTuple() {
    return currentTuple;
  }

  static final class TuplePrimitiveConverter extends PrimitiveConverter {
    private final TupleConverter parent;
    private final int index;

    public TuplePrimitiveConverter(TupleConverter parent, int index) {
      this.parent = parent;
      this.index = index;
    }

    @Override
    public void addBinary(Binary value) {
      parent.getCurrentTuple().setString(index, value.toStringUsingUTF8());
    }

    @Override
    public void addBoolean(boolean value) {
      parent.getCurrentTuple().setBoolean(index, value);
    }

    @Override
    public void addDouble(double value) {
      parent.getCurrentTuple().setDouble(index, value);
    }

    @Override
    public void addFloat(float value) {
      parent.getCurrentTuple().setFloat(index, value);
    }

    @Override
    public void addInt(int value) {
      parent.getCurrentTuple().setInteger(index, value);
    }

    @Override
    public void addLong(long value) {
      parent.getCurrentTuple().setLong(index, value);
    }
  }
}