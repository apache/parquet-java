package parquet.proto.converters;

import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

public final class ProtobufStringConverter extends PrimitiveConverter {

  final ParentValueContainer parent;

  public ProtobufStringConverter(ParentValueContainer parent) {
    this.parent = parent;
  }


  @Override
  public void addBinary(Binary value) {
    parent.add(value.toStringUsingUTF8());
  }

}
