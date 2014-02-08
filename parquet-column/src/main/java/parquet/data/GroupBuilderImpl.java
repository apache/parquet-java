package parquet.data;

import parquet.data.materializer.GroupMaterializer;
import parquet.io.api.Binary;
import parquet.io.api.GroupConverter;
import parquet.schema.GroupType;
import parquet.schema.MessageType;

class GroupBuilderImpl extends GroupBuilder {

  protected final GroupConverter converter;
  protected final GroupType type;
  private final GroupBuilderImpl parent;
  protected boolean enabled = true;

  public static GroupBuilder newGroupBuilderImpl(MessageType schema) {
    final GroupMaterializer materializer = new GroupMaterializer(schema);
    return new GroupBuilderImpl(materializer.getRootConverter(), schema, null) {
      @Override
      public GroupBuilder startMessage() {
        converter.start();
        return new GroupBuilderImpl(converter, type, this);
      }

      @Override
      public Group endMessage() {
        converter.end();
        return materializer.getCurrentRecord();
      }
    };
  }

  private GroupBuilderImpl(GroupConverter converter, GroupType type, GroupBuilderImpl parent) {
    this.converter = converter;
    this.type = type;
    this.parent = parent;
  }

  @Override
  public GroupType getType() {
    return type;
  }

  @Override
  public GroupBuilder startMessage() {
    throw new UnsupportedOperationException("not the root");
  }

  @Override
  public Group endMessage() {
    throw new UnsupportedOperationException("not the root");
  }

  @Override
  public GroupBuilder startGroup(int fieldIndex) {
    GroupConverter child = converter.getConverter(fieldIndex).asGroupConverter();
    child.start();
    this.enabled = false;
    return new GroupBuilderImpl(child, type.getType(fieldIndex).asGroupType(), this);
  }

  @Override
  public GroupBuilder endGroup() {
    converter.end();
    this.enabled = false;
    parent.enabled = true;
    return parent;
  }

  @Override
  public GroupBuilder addIntValue(int fieldIndex, int value) {
    converter.getConverter(fieldIndex).asPrimitiveConverter().addInt(value);
    return this;
  }

  @Override
  public GroupBuilder addLongValue(int fieldIndex, long value) {
    converter.getConverter(fieldIndex).asPrimitiveConverter().addLong(value);
    return this;
  }

  @Override
  public GroupBuilder addBooleanValue(int fieldIndex, boolean value) {
    converter.getConverter(fieldIndex).asPrimitiveConverter().addBoolean(value);
    return this;
  }

  @Override
  public GroupBuilder addFloatValue(int fieldIndex, float value) {
    converter.getConverter(fieldIndex).asPrimitiveConverter().addFloat(value);
    return this;
  }

  @Override
  public GroupBuilder addDoubleValue(int fieldIndex, double value) {
    converter.getConverter(fieldIndex).asPrimitiveConverter().addDouble(value);
    return this;
  }

  @Override
  public GroupBuilder addBinaryValue(int fieldIndex, Binary value) {
    converter.getConverter(fieldIndex).asPrimitiveConverter().addBinary(value);
    return this;
  }

}
