package redelm.data;

import redelm.schema.GroupType;

abstract public class GroupValueSource {

  abstract public int getFieldRepetitionCount(String field);

  abstract public GroupValueSource getGroup(String field, int index);

  abstract public String getString(String field, int index);

  abstract public int getInt(String field, int index);

  abstract public boolean getBool(String field, int index);

  abstract public byte[] getBinary(String field, int index);

  abstract public GroupType getType();
}
