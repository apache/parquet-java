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
import java.util.Arrays;
import java.util.List;

import parquet.column.Encoding;
import parquet.schema.PrimitiveType.PrimitiveTypeName;


/**
 * Column meta data for a block stored in the file footer and passed in the InputSplit
 * @author Julien Le Dem
 *
 */
public class ColumnChunkMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  private final CompressionCodecName codec;
  private final String[] path;
  private final PrimitiveTypeName type;
  private final List<Encoding> encodings;

  private long firstDataPage;

  // for info
  private long valueCount;
  private long totalSize;

  private long totalUncompressedSize;


  /**
   *
   * @param path column identifier
   * @param type type of the column
   * @param encodings
   */
  public ColumnChunkMetaData(String[] path, PrimitiveTypeName type, CompressionCodecName codec, List<Encoding> encodings) {
    this.path = path;
    this.type = type;
    this.codec = codec;
    this.encodings = encodings;
  }

  public CompressionCodecName getCodec() {
    return codec;
  }

  /**
   *
   * @return column identifier
   */
  public String[] getPath() {
    return path;
  }

  /**
   *
   * @return type of the column
   */
  public PrimitiveTypeName getType() {
    return type;
  }

  /**
   *
   * @param dataStart offset in the file where data starts
   */
  public void setFirstDataPageOffset(long firstDataPage) {
    this.firstDataPage = firstDataPage;
  }

  /**
   *
   * @return start of the column data offset
   */
  public long getFirstDataPageOffset() {
    return firstDataPage;
  }

  /**
   *
   * @param valueCount count of values in this block of the column
   */
  public void setValueCount(long valueCount) {
    this.valueCount = valueCount;
  }

  /**
   *
   * @return count of values in this block of the column
   */
  public long getValueCount() {
    return valueCount;
  }

  /**
   * @return the totalUncompressedSize
   */
  public long getTotalUncompressedSize() {
    return totalUncompressedSize;
  }

  /**
   * @param totalUncompressedSize the totalUncompressedSize to set
   */
  public void setTotalUncompressedSize(long totalUncompressedSize) {
    this.totalUncompressedSize = totalUncompressedSize;
  }
  /**
   * @return the totalSize
   */
  public long getTotalSize() {
    return totalSize;
  }

  /**
   * @param totalSize the totalSize to set
   */
  public void setTotalSize(long totalSize) {
    this.totalSize = totalSize;
  }

  /**
   *
   * @return all the encodings used in this column
   */
  public List<Encoding> getEncodings() {
    return encodings;
  }

  @Override
  public String toString() {
    return "ColumnMetaData{" + codec + ", " + firstDataPage + ", " + Arrays.toString(path) + "}";
  }

}
