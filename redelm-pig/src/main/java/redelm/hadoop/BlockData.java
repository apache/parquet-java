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

import java.util.List;

/**
 * Contains the data of a block in raw form
 *
 * @author Julien Le Dem
 *
 */
class BlockData {

  private final int recordCount;
  private final List<ColumnData> columns;

  public BlockData(int recordCount, List<ColumnData> columns) {
    this.recordCount = recordCount;
    this.columns = columns;
  }

  /**
   *
   * @return count of records in this block
   */
  public int getRecordCount() {
    return recordCount;
  }

  /**
   * column data for this block
   * @return
   */
  public List<ColumnData> getColumns() {
    return columns;
  }

}
