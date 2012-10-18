package redelm.pig;

import java.io.Serializable;
import java.util.Arrays;

import redelm.schema.PrimitiveType.Primitive;

public class ColumnMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  private final long startIndex;
  private long endIndex;
  private final String[] path;
  private final Primitive type;
  private int recordCount;

  public ColumnMetaData(long startIndex, String[] path, Primitive type) {
    this.startIndex = startIndex;
    this.path = path;
    this.type = type;
  }

  public void setEndIndex(long endIndex) {
    this.endIndex = endIndex;
  }

  public long getStartIndex() {
    return startIndex;
  }

  public long getEndIndex() {
    return endIndex;
  }

  public String[] getPath() {
    return path;
  }

  public Primitive getType() {
    return type;
  }

  public int getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  @Override
  public String toString() {
    return "ColumnMetaData{" + startIndex + ", " + endIndex + " " + Arrays.toString(path) + "}";
  }

}
