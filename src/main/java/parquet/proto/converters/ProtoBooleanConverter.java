package parquet.proto.converters;

import parquet.io.api.PrimitiveConverter;

public final class ProtoBooleanConverter extends PrimitiveConverter {

  final ParentValueContainer parent;

  public ProtoBooleanConverter(ParentValueContainer parent) {
    this.parent = parent;
  }

  @Override
  final public void addBoolean(boolean value) {
    parent.add(value ? Boolean.TRUE : Boolean.FALSE);
  }

}
