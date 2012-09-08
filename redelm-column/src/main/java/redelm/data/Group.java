package redelm.data;

import java.util.logging.Logger;

import redelm.Log;

abstract public class Group extends GroupValueSource {
  private static final Logger logger = Logger.getLogger(Group.class.getName());
  private static final boolean DEBUG = Log.DEBUG;

  public void add(String field, int value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, String value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, boolean value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, byte[] value) {
    add(getType().getFieldIndex(field), value);
  }

  public Group addGroup(String field) {
    if (DEBUG) logger.fine("add group "+field+" to "+getType().getName());
    return addGroup(getType().getFieldIndex(field));
  }

  public Group getGroup(String field, int index) {
    return getGroup(getType().getFieldIndex(field), index);
  }

  abstract public void add(int fieldIndex, int value);

  abstract public void add(int fieldIndex, String value);

  abstract public void add(int fieldIndex, boolean value);

  abstract public void add(int fieldIndex, byte[] value);

  abstract public Group addGroup(int fieldIndex);

  abstract public Group getGroup(int fieldIndex, int index);

  public Group asGroup() {
    return this;
  }

  public Group append(String fieldName, int value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, String value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, boolean value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, byte[] value) {
    add(fieldName, value);
    return this;
  }

}
