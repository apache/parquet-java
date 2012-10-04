package redelm.pig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BlockMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  private final long startIndex;
  private long endIndex;
  private List<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
  private int recordCount;

  public BlockMetaData(long startIndex) {
    this.startIndex = startIndex;
  }

  public void setEndIndex(long endIndex) {
    this.endIndex = endIndex;
  }

  public void addColumn(ColumnMetaData column) {
    columns.add(column);
  }

  public long getStartIndex() {
    return startIndex;
  }

  public long getEndIndex() {
    return endIndex;
  }

  public List<ColumnMetaData> getColumns() {
    return columns;
  }

  @Override
  public String toString() {
    return "BlockMetaData{" + startIndex + ", " + endIndex + " " + columns + "}";
  }

  public int getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }
}
