package parquet.data.materializer;

import static parquet.schema.Type.Repetition.REPEATED;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import parquet.data.Group;
import parquet.io.api.Binary;
import parquet.schema.Type;

public abstract class CompiledGroup extends Group {

  // TODO: move down to enable not having this
  protected BitSet isSet = new BitSet();

  public CompiledGroup() {
  }

  protected RuntimeException error(int fieldIndex) {
    return new RuntimeException(getClass().getName() + " Error, field index " + fieldIndex + " not found for this type: " + getType());
  }

  @Override
  public String toString() {
    return toString("");
  }

  public String toString(String indent) {
    String result = "";
    int fieldIndex = 0;
    for (Type field : getType().getFields()) {
      String name = field.getName();
      List<Object> values;
      if (field.isRepetition(REPEATED)) {
        int count = this.getRepetitionCount(fieldIndex);
        values = new ArrayList<Object>(count);
        for (int index = 0; index < count; index++) {
          values.add(getRepeatedValueAt(fieldIndex, index));
        }
      } else {
        values = new ArrayList<Object>(1);
        if (isDefined(fieldIndex)) {
          values.add(getValue(fieldIndex));
        }
      }
      ++ fieldIndex;
      for (Object value : values) {
        result += indent + name;
        if (value == null) {
          result += ": NULL\n";
        } else if (value instanceof Group) {
          result += "\n" + ((CompiledGroup)value).toString(indent+"  ");
        } else if (value instanceof Binary) {
          result += ": " + ((Binary)value).toStringUsingUTF8() + "\n";
        } else {
          result += ": " + value.toString() + "\n";
        }
      }
    }
    return result;
  }

  // for repeated fields
  @Override
  public int getRepetitionCount(int fieldIndex) {
    throw error(fieldIndex);
  }

  @Override
  public Group getRepeatedGroupAt(int fieldIndex, int index) {
    throw error(fieldIndex);
  }

  @Override
  public int getRepeatedIntAt(int fieldIndex, int index) {
    throw error(fieldIndex);
  }

  @Override
  public long getRepeatedLongAt(int fieldIndex, int index) {
    throw error(fieldIndex);
  }

  @Override
  public float getRepeatedFloatAt(int fieldIndex, int index) {
    throw error(fieldIndex);
  }

  @Override
  public double getRepeatedDoubleAt(int fieldIndex, int index) {
    throw error(fieldIndex);
  }

  @Override
  public boolean getRepeatedBooleanAt(int fieldIndex, int index) {
    throw error(fieldIndex);
  }

  @Override
  public Binary getRepeatedBinaryAt(int fieldIndex, int index) {
    throw error(fieldIndex);
  }

  @Override
  public Object getRepeatedValueAt(int fieldIndex, int index) {
    throw error(fieldIndex);
  }

  // for non-repeated fields

  @Override
  public boolean isDefined(int fieldIndex) {
    throw error(fieldIndex);
  }

  @Override
  public Group getGroup(int fieldIndex) {
    throw error(fieldIndex);
  }

  @Override
  public int getInt(int fieldIndex) {
    throw error(fieldIndex);
  }

  @Override
  public long getLong(int fieldIndex) {
    throw error(fieldIndex);
  }

  @Override
  public float getFloat(int fieldIndex) {
    throw error(fieldIndex);
  }

  @Override
  public double getDouble(int fieldIndex) {
    throw error(fieldIndex);
  }

  @Override
  public boolean getBoolean(int fieldIndex) {
    throw error(fieldIndex);
  }

  @Override
  public Binary getBinary(int fieldIndex) {
    throw error(fieldIndex);
  }

  @Override
  public Object getValue(int fieldIndex) {
    throw error(fieldIndex);
  }

}
