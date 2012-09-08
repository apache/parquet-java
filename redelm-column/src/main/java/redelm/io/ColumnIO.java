package redelm.io;


import java.util.Arrays;
import java.util.List;

import redelm.Log;
import redelm.column.ColumnsStore;
import redelm.data.GroupValueSource;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

abstract public class ColumnIO {

  static final boolean DEBUG = Log.DEBUG;

  private final GroupColumnIO parent;
  private final Type type;
  private int repetitionLevel;
  private int definitionLevel;
  private String[] fieldPath;
  private int[] indexFieldPath;

  ColumnIO(Type type, GroupColumnIO parent) {
    this.type = type;
    this.parent = parent;
  }

  String[] getFieldPath() {
    return fieldPath;
  }

  public int[] getIndexFieldPath() {
    return indexFieldPath;
  }

  int getRepetitionLevel() {
    return repetitionLevel;
  }

  int getDefinitionLevel() {
    return definitionLevel;
  }

  void setRepetitionLevel(int repetitionLevel) {
    this.repetitionLevel = repetitionLevel;
  }

  void setDefinitionLevel(int definitionLevel) {
    this.definitionLevel = definitionLevel;
  }

  void setFieldPath(String[] fieldPath, int[] indexFieldPath) {
    this.fieldPath = fieldPath;
    this.indexFieldPath = indexFieldPath;
  }

  abstract void writeValue(GroupValueSource parent, String field, int index, int r, int d);

  abstract void writeNull(int r, int d);

  void writeValuesForField(GroupValueSource parent, String field, int r, int d) {
    int fieldRepetitionCount = parent.getFieldRepetitionCount(field);
    if (fieldRepetitionCount > 0) {
      // first value: keeps current repetitionLevel r
      writeValue(parent, field, 0, r, this.getDefinitionLevel());
      for (int i = 1; i < fieldRepetitionCount; i++) {
        // other values are repeated: set repetitionLevel for this level
        writeValue(parent, field, i, this.getRepetitionLevel(), this.getDefinitionLevel());
      }
    } else {
      // first value: keeps current repetitionLevel r
      // missing value: we pass the last known definition level d
      writeNull(r, d);
    }
  }

  Type getType() {
    return type;
  }

  void setLevels(int r, int d, String[] fieldPath, int[] indexFieldPath, List<ColumnIO> repetition, List<ColumnIO> path, ColumnsStore columns) {
    setRepetitionLevel(r);
    setDefinitionLevel(d);
    setFieldPath(fieldPath, indexFieldPath);
  }

  abstract List<String[]> getColumnNames();

  public GroupColumnIO getParent() {
    return parent;
  }

  abstract PrimitiveColumnIO getLast();
  abstract PrimitiveColumnIO getFirst();

  ColumnIO getParent(int r) {
    if (getRepetitionLevel() == r && getType().getRepetition() == Repetition.REPEATED) {
      return this;
    } else  if (getParent()!=null && getParent().getDefinitionLevel()>=r) {
      return getParent().getParent(r);
    } else {
      throw new RuntimeException("no parent("+r+") for "+Arrays.toString(this.getFieldPath()));
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName()+" "+type.getName()
        +" r:"+repetitionLevel
        +" d:"+definitionLevel
        +" "+Arrays.toString(fieldPath);
  }

}
