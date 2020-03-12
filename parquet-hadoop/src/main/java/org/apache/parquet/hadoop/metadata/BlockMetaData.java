/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Block metadata stored in the footer and passed in an InputSplit
 */
public class BlockMetaData {

  private List<ColumnChunkMetaData> columns = new ArrayList<ColumnChunkMetaData>();
  private long rowCount;
  private long totalByteSize;
  private String path;
  
  private short ordinal;
  // TODO private? all 4
  long startingPosition;
  long totalCompressedSize;
  boolean startingPositionSet;
  boolean totalCompressedSizeSet;

  public BlockMetaData() {
    startingPositionSet = false;
    totalCompressedSizeSet = false;
  }
  
  // Reader side (get parameters from RowGroup structure)
  public BlockMetaData(long fileOffset, long totalCompressedSize) {
    // fileOffset is optional, 0 means fileOffset is absent
    if (fileOffset > 0) {
      this.startingPosition = fileOffset;
      startingPositionSet = true;
    }

    // totalCompressedSize is optional, 0 means totalCompressedSize is absent
    if (totalCompressedSize > 0) {
      this.totalCompressedSize = totalCompressedSize;
      totalCompressedSizeSet = true;
    }
  }


  /**
   * @param path the path to the file containing the data. Or null if same file the metadata was found
   */
  public void setPath(String path) {
    this.path = path;
  }

  /**
   * @return the path relative to the parent of this file where the data is. Or null if it is in the same file.
   */
  public String getPath() {
    return path;
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
    return Collections.unmodifiableList(columns);
  }

  /**
   *
   * @return the starting pos of first column
   */
  public long getStartingPos() {
    if (startingPositionSet) return startingPosition;
    return getColumns().get(0).getStartingPos(); // TODO store in startingPosition
  }
  
  @Override
  public String toString() {
    return "BlockMetaData{" + rowCount + ", " + totalByteSize + " " + columns + "}";
  }

  /**
   * @return the compressed size of all columns
   */
  public long getCompressedSize() {
    if (totalCompressedSizeSet) return totalCompressedSize;
    long totalSize = 0;
    for (ColumnChunkMetaData col : getColumns()) {
      totalSize += col.getTotalSize();
    }
    return totalSize; // TODO store in totalByteSize
  }
  
  public short getOrdinal() {
    return ordinal;
  }

  public void setOrdinal(short ordinal) {
    this.ordinal = ordinal;
  }
}
