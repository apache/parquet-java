package parquet.proto.converters;

import parquet.io.api.PrimitiveConverter;

public final class ProtoFloatConverter extends PrimitiveConverter {

  final ParentValueContainer parent;

  public ProtoFloatConverter(ParentValueContainer parent) {
    this.parent = parent;
  }

  @Override
  public void addFloat(float value) {
    parent.add(value);
  }
}
