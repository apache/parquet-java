package parquet.data.materializer;

import static parquet.schema.Type.Repetition.REPEATED;

import java.util.ArrayList;
import java.util.List;

import parquet.data.Group;
import parquet.io.api.Binary;
import parquet.schema.Type;

public abstract class CompiledGroup extends Group {

  public CompiledGroup() {
  }

  protected Exception error(int fieldIndex) {
    return new RuntimeException("Error, field index not found for this type: " + fieldIndex);
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

}
