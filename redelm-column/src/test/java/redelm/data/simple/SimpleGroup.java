package redelm.data.simple;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import redelm.Log;
import redelm.data.Group;
import redelm.schema.GroupType;
import redelm.schema.Type;

public class SimpleGroup extends Group {
  private static final Logger logger = Logger.getLogger(SimpleGroup.class.getName());

  private static final boolean DEBUG = Log.DEBUG;

  private final GroupType schema;
  private final List<Object>[] data;

  @SuppressWarnings("unchecked")
  public SimpleGroup(GroupType schema) {
    this.schema = schema;
    this.data = new List[schema.getFields().size()];
    for (int i = 0; i < schema.getFieldCount(); i++) {
       this.data[i] = new ArrayList<Object>();
    }
  }

  @Override
  public String toString() {
    return toString("");
  }

  public String toString(String indent) {
    String result = "";
    int i = 0;
    for (Type field : schema.getFields()) {
      String name = field.getName();
      List<Object> values = data[i];
      ++i;
      if (values != null) {
        if (values.size() > 0) {
          for (Object value : values) {
            result += indent + name;
            if (value == null) {
              result += ": NULL\n";
            } else if (value instanceof Group) {
              result += "\n" + ((SimpleGroup)value).toString(indent+"  ");
            } else {
              result += ": " + value.toString() + "\n";
            }
          }
        }
      }
    }
    return result;
  }

  @Override
  public Group addGroup(String field) {
    if (DEBUG) logger.fine("add group "+field+" to "+schema.getName());
    SimpleGroup g = new SimpleGroup(schema.getType(field).asGroupType());
    data[schema.getFieldIndex(field)].add(g);
    return g;
  }

  @Override
  public Group getGroup(String field, int index) {
    return (Group)getValue(field, index);
  }

  private Object getValue(String field, int index) {
    try {
      return data[schema.getFieldIndex(field)].get(index);
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException("not found " + field + "("+schema.getFieldIndex(field)+") element number "+index+" in group:\n"+this);
    }
  }

  private void add(String field, Primitive value) {
    Type type = schema.getType(field);
    List<Object> list = data[schema.getFieldIndex(field)];
    if (type.getRepetition() != Type.Repetition.REPEATED
        && !list.isEmpty()) {
      throw new IllegalStateException("field "+field+" can not have more than one value: " + list);
    }
    list.add(value);
  }

  @Override
  public int getFieldRepetitionCount(String field) {
    List<Object> list = data[schema.getFieldIndex(field)];
    return list == null ? 0 : list.size();
  }

  @Override
  public String getString(String field, int index) {
    return ((StringValue)getValue(field, index)).getString();
  }

  @Override
  public int getInt(String field, int index) {
    return ((IntValue)getValue(field, index)).getInt();
  }

  @Override
  public boolean getBool(String field, int index) {
    return ((BoolValue)getValue(field, index)).getBool();
  }

  @Override
  public byte[] getBinary(String field, int index) {
    return ((BinaryValue)getValue(field, index)).getBinary();
  }

  @Override
  public void add(String field, int value) {
    add(field, new IntValue(value));
  }

  @Override
  public void add(String field, String value) {
    add(field, new StringValue(value));
  }

  @Override
  public void add(String field, boolean value) {
    add(field, new BoolValue(value));
  }

  @Override
  public void add(String field, byte[] value) {
    add(field, new BinaryValue(value));
  }

  @Override
  public GroupType getType() {
    return schema;
  }

}
