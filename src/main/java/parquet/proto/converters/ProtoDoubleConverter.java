package parquet.proto.converters;

import parquet.io.api.PrimitiveConverter;

public final class ProtoDoubleConverter extends PrimitiveConverter {

  final ParentValueContainer parent;

  public ProtoDoubleConverter(ParentValueContainer parent) {
    this.parent = parent;
  }

  @Override
  public void addDouble(double value) {
    parent.add(value);
  }
}
