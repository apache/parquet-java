package redelm.pig;

import java.util.List;

public class BlockData {

  private final int recordCount;
  private final List<ColumnData> columns;

  public BlockData(int recordCount, List<ColumnData> columns) {
    this.recordCount = recordCount;
    this.columns = columns;
  }

  public int getRecordCount() {
    return recordCount;
  }

  public List<ColumnData> getColumns() {
    return columns;
  }

}
