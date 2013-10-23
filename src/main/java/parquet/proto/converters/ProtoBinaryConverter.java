package parquet.proto.converters;

import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

public final class ProtoBinaryConverter extends PrimitiveConverter {

  final ParentValueContainer parent;

  public ProtoBinaryConverter(ParentValueContainer parent) {
    this.parent = parent;
  }

  @Override
  public void addBinary(Binary value) {
    parent.add(value);
  }
}
