package parquet.data;

import parquet.io.api.Binary;
import parquet.schema.GroupType;

abstract public class GroupBuilder {

  abstract public GroupType getType();

  abstract public GroupBuilder startMessage();

  abstract public Group endMessage();

  // === int based field access ===

  abstract public GroupBuilder startGroup(int fieldIndex);

  abstract public GroupBuilder endGroup();

  abstract public GroupBuilder addIntValue(int fieldIndex, int value);

  abstract public GroupBuilder addLongValue(int fieldIndex, long value);

  abstract public GroupBuilder addBooleanValue(int fieldIndex, boolean value);

  abstract public GroupBuilder addFloatValue(int fieldIndex, float value);

  abstract public GroupBuilder addDoubleValue(int fieldIndex, double value);

  abstract public GroupBuilder addBinaryValue(int fieldIndex, Binary value);

  // === name based field access ===

  public GroupBuilder startGroup(String field) {
    return startGroup(getType().getFieldIndex(field));
  }

  public GroupBuilder addIntValue(String field, int value) {
    return addIntValue(getType().getFieldIndex(field), value);
  }

  public GroupBuilder addLongValue(String field, long value) {
    return addLongValue(getType().getFieldIndex(field), value);
  }

  public GroupBuilder addBooleanValue(String field, boolean value) {
    return addBooleanValue(getType().getFieldIndex(field), value);
  }

  public GroupBuilder addFloatValue(String field, float value) {
    return addFloatValue(getType().getFieldIndex(field), value);
  }

  public GroupBuilder addDoubleValue(String field, double value) {
    return addDoubleValue(getType().getFieldIndex(field), value);
  }

  public GroupBuilder addBinaryValue(String field, Binary value) {
    return addBinaryValue(getType().getFieldIndex(field), value);
  }

}
