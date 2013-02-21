/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * Block metadata stored in the footer and passed in an InputSplit
 *
 * @author Julien Le Dem
 *
 */
public class BlockMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  private List<ColumnChunkMetaData> columns = new ArrayList<ColumnChunkMetaData>();
  private long rowCount;
  private long totalByteSize;

  public BlockMetaData() {
  }

  /**
   * @return the rowCount
   */
  public long getRowCount() {
    return rowCount;
  }

  /**
   * @param rowCount the rowCount to set
   */
  public void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  /**
   * @return the totalByteSize
   */
  public long getTotalByteSize() {
    return totalByteSize;
  }

  /**
   * @param totalByteSize the totalByteSize to set
   */
  public void setTotalByteSize(long totalByteSize) {
    this.totalByteSize = totalByteSize;
  }

  /**
   *
   * @param column the metadata for a column
   */
  public void addColumn(ColumnChunkMetaData column) {
    columns.add(column);
  }

  /**
   *
   * @return the metadata for columns
   */
  public List<ColumnChunkMetaData> getColumns() {
    return columns;
  }

  @Override
  public String toString() {
    return "BlockMetaData{" + rowCount + ", " + totalByteSize + " " + columns + "}";
  }

}
