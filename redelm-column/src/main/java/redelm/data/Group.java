package redelm.data;

abstract public class Group extends GroupValueSource {

  abstract public void add(String field, int value);

  abstract public void add(String field, String value);

  abstract public void add(String field, boolean value);

  abstract public void add(String field, byte[] value);

  abstract public Group addGroup(String field);

  abstract public Group getGroup(String field, int index);

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
