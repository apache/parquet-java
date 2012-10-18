package redelm.pig;

import java.io.Serializable;
import java.util.Arrays;

import redelm.schema.PrimitiveType.Primitive;

public class ColumnMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  private long repetitionStart;
  private long definitionStart;
  private long dataStart;
  private long dataEnd;
  private final String[] path;
  private final Primitive type;
  private int valueCount;

  public ColumnMetaData(String[] path, Primitive type) {
    this.path = path;
    this.type = type;
  }

  public String[] getPath() {
    return path;
  }

  public Primitive getType() {
    return type;
  }

  @Override
  public String toString() {
    return "ColumnMetaData{" + repetitionStart + ", " + definitionStart + ", " + dataStart + ", " + dataEnd + " " + Arrays.toString(path) + "}";
  }


  public void setRepetitionStart(long repetitionStart) {
    this.repetitionStart = repetitionStart;
  }

  public void setDefinitionStart(long definitionStart) {
    this.definitionStart = definitionStart;
  }

  public void setDataStart(long dataStart) {
    this.dataStart = dataStart;
  }

  public void setDataEnd(long dataEnd) {
    this.dataEnd = dataEnd;
  }

  public long getColumnStart() {
    return repetitionStart;
  }

  public long getRepetitionStart() {
    return repetitionStart;
  }

  public long getRepetitionEnd() {
    return definitionStart;
  }

  public long getRepetitionLength() {
    return getRepetitionEnd() - getRepetitionStart();
  }

  public long getDefinitionStart() {
    return definitionStart;
  }

  public long getDefinitionEnd() {
    return dataStart;
  }

  public long getDefinitionLength() {
    return getDefinitionEnd() - getDefinitionStart();
  }

  public long getDataStart() {
    return dataStart;
  }

  public long getDataEnd() {
    return dataEnd;
  }

  public long getDataLength() {
    return getDataEnd() - getDataStart();
  }

  public long getColumnEnd() {
    return dataEnd;
  }

  public long getColumnLength() {
    return getColumnEnd() - getColumnStart();
  }

  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
  }

  public int getValueCount() {
    return valueCount;
  }

}
