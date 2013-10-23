package parquet.proto.converters;

import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;

public class ProtoArrayConverter extends GroupConverter {

  private final Converter converter;

  public ProtoArrayConverter(Converter innerConverter) {
    converter = innerConverter;
  }

  @Override
  public Converter getConverter(int i) {
    return converter;
  }

  @Override
  public void start() {

  }

  @Override
  public void end() {

  }
}
