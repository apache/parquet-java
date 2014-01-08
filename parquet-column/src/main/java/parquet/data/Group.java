package parquet.data;

import parquet.io.api.Binary;
import parquet.schema.GroupType;

/**
 * Defines the canonical group type
 *
 * @author Julien Le Dem
 *
 */
abstract public class Group {

  // === name based field access ===

  // for repeated fields
  public int getRepetitionCount(String field) {
    return getRepetitionCount(getType().getFieldIndex(field));
  }

  public Group getRepeatedGroupAt(String field, int index) {
    return getRepeatedGroupAt(getType().getFieldIndex(field), index);
  }

  public int getRepeatedIntegerAt(String field, int index) {
    return getRepeatedIntegerAt(getType().getFieldIndex(field), index);
  }

  public long getRepeatedLongAt(String field, int index) {
    return getRepeatedLongAt(getType().getFieldIndex(field), index);
  }

  public float getRepeatedFloatAt(String field, int index) {
    return getRepeatedFloatAt(getType().getFieldIndex(field), index);
  }

  public double getRepeatedDoubleAt(String field, int index) {
    return getRepeatedDoubleAt(getType().getFieldIndex(field), index);
  }

  public boolean getRepeatedBooleanAt(String field, int index) {
    return getRepeatedBooleanAt(getType().getFieldIndex(field), index);
  }

  public Binary getRepeatedBinaryAt(String field, int index) {
    return getRepeatedBinaryAt(getType().getFieldIndex(field), index);
  }

  public Object getRepeatedValueAt(String field, int index) {
    return getRepeatedValueAt(getType().getFieldIndex(field), index);
  }

  // for non-repeated fields

  public boolean isDefined(String field) {
    return isDefined(getType().getFieldIndex(field));
  }

  public Group getGroup(String field) {
    return getGroup(getType().getFieldIndex(field));
  }

  public int getInteger(String field) {
    return getInteger(getType().getFieldIndex(field));
  }

  public long getLong(String field) {
    return getLong(getType().getFieldIndex(field));
  }

  public float getFloat(String field) {
    return getFloat(getType().getFieldIndex(field));
  }

  public double getDouble(String field) {
    return getDouble(getType().getFieldIndex(field));
  }

  public boolean getBoolean(String field) {
    return getBoolean(getType().getFieldIndex(field));
  }

  public Binary getBinary(String field) {
    return getBinary(getType().getFieldIndex(field));
  }

  public Object getValue(String field) {
    return getValue(getType().getFieldIndex(field));
  }

  // === index based field access ===

  // for repeated fields
  abstract public int getRepetitionCount(int fieldIndex);

  abstract public Group getRepeatedGroupAt(int fieldIndex, int index);

  abstract public int getRepeatedIntegerAt(int fieldIndex, int index);

  abstract public long getRepeatedLongAt(int fieldIndex, int index);

  abstract public float getRepeatedFloatAt(int fieldIndex, int index);

  abstract public double getRepeatedDoubleAt(int fieldIndex, int index);

  abstract public boolean getRepeatedBooleanAt(int fieldIndex, int index);

  abstract public Binary getRepeatedBinaryAt(int fieldIndex, int index);

  abstract public Object getRepeatedValueAt(int fieldIndex, int index);

  // for non-repeated fields

  abstract public boolean isDefined(int fieldIndex);

  abstract public Group getGroup(int fieldIndex);

  abstract public int getInteger(int fieldIndex);

  abstract public long getLong(int fieldIndex);

  abstract public float getFloat(int fieldIndex);

  abstract public double getDouble(int fieldIndex);

  abstract public boolean getBoolean(int fieldIndex);

  abstract public Binary getBinary(int fieldIndex);

  abstract public Object getValue(int fieldIndex);

  // type of the group

  abstract public GroupType getType();
}
