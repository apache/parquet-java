package parquet.proto.converters;

import parquet.io.api.PrimitiveConverter;

public final class ProtoIntConverter extends PrimitiveConverter {

  final ParentValueContainer parent;

  public ProtoIntConverter(ParentValueContainer parent) {
    this.parent = parent;
  }

  @Override
  public void addInt(int value) {
    parent.add(value);
  }
}
