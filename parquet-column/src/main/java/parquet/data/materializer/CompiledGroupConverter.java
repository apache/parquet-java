package parquet.data.materializer;

import java.util.BitSet;

import parquet.data.Group;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;

public abstract class CompiledGroupConverter extends GroupConverter {

  public CompiledGroupConverter() {
  }

  protected BitSet isSet = new BitSet();

  Converter[] converters;

  public abstract Group getCurrentRecord();

  @Override
  public Converter getConverter(int fieldIndex) {
    return (Converter)converters[fieldIndex];
  }

  @Override
  public void start() {
  }

}
