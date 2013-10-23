package parquet.proto.converters;

import parquet.io.api.PrimitiveConverter;

public final class ProtoLongConverter extends PrimitiveConverter {

  final ParentValueContainer parent;

  public ProtoLongConverter(ParentValueContainer parent) {
    this.parent = parent;
  }

  @Override
  public void addLong(long value) {
    parent.add(value);
  }
}
