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
package redelm.hadoop;

import java.io.Serializable;
import java.util.ArrayList;
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
