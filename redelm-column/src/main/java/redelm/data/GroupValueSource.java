package redelm.data;

import redelm.schema.GroupType;

abstract public class GroupValueSource {

  public int getFieldRepetitionCount(String field) {
    return getFieldRepetitionCount(getType().getFieldIndex(field));
  }

  public GroupValueSource getGroup(String field, int index) {
    return getGroup(getType().getFieldIndex(field), index);
  }

  public String getString(String field, int index) {
    return getString(getType().getFieldIndex(field), index);
  }

  public int getInt(String field, int index) {
    return getInt(getType().getFieldIndex(field), index);
  }

  public boolean getBool(String field, int index) {
    return getBool(getType().getFieldIndex(field), index);
  }

  public byte[] getBinary(String field, int index) {
    return getBinary(getType().getFieldIndex(field), index);
  }

  abstract public int getFieldRepetitionCount(int fieldIndex);

  abstract public GroupValueSource getGroup(int fieldIndex, int index);

  abstract public String getString(int fieldIndex, int index);

  abstract public int getInt(int fieldIndex, int index);

  abstract public boolean getBool(int fieldIndex, int index);

  abstract public byte[] getBinary(int fieldIndex, int index);

  abstract public GroupType getType();
}
