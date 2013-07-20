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

import java.util.Set;

import parquet.column.Encoding;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * Column meta data for a block stored in the file footer and passed in the InputSplit
 * @author Julien Le Dem
 */
abstract public class ColumnChunkMetaData {

  public static ColumnChunkMetaData get(
      ColumnPath path, PrimitiveTypeName type, CompressionCodecName codec, Set<Encoding> encodings,
      long firstDataPage,
      long dictionaryPageOffset,
      long valueCount,
      long totalSize,
      long totalUncompressedSize) {
    if (firstDataPage < Integer.MAX_VALUE
        && dictionaryPageOffset < Integer.MAX_VALUE
        && valueCount < Integer.MAX_VALUE
        && totalSize < Integer.MAX_VALUE
        && totalUncompressedSize < Integer.MAX_VALUE) {
      return new IntColumnChunkMetaData(
          path, type, codec, encodings,
          firstDataPage,
          dictionaryPageOffset,
          valueCount,
          totalSize,
          totalUncompressedSize);
    } else {
      return new LongColumnChunkMetaData(
          path, type, codec, encodings,
          firstDataPage,
          dictionaryPageOffset,
          valueCount,
          totalSize,
          totalUncompressedSize);
    }
  }

  // we save 3 references by storing together the column properties that have few distinct values
  private final ColumnChunkProperties properties;

  protected ColumnChunkMetaData(ColumnChunkProperties columnChunkProperties) {
    this.properties = columnChunkProperties;
  }

  public CompressionCodecName getCodec() {
    return properties.getCodec();
  }

  /**
   *
   * @return column identifier
   */
  public ColumnPath getPath() {
    return properties.getPath();
  }

  /**
   * @return type of the column
   */
  public PrimitiveTypeName getType() {
    return properties.getType();
  }


  /**
   * @return start of the column data offset
   */
  abstract public long getFirstDataPageOffset();

  /**
   * @return the location of the dictionary page if any
   */
  abstract public long getDictionaryPageOffset();

  /**
   * @return count of values in this block of the column
   */
  abstract public long getValueCount();

  /**
   * @return the totalUncompressedSize
   */
  abstract public long getTotalUncompressedSize();

  /**
   * @return the totalSize
   */
  abstract public long getTotalSize();

  /**
   * @return all the encodings used in this column
   */
  public Set<Encoding> getEncodings() {
    return properties.getEncodings();
  }

  @Override
  public String toString() {
    return "ColumnMetaData{" + properties.toString() + ", " + getFirstDataPageOffset() + "}";
  }
}
class IntColumnChunkMetaData extends ColumnChunkMetaData {

  private final int firstDataPage;
  private final int dictionaryPageOffset;
  private final int valueCount;
  private final int totalSize;
  private final int totalUncompressedSize;

  /**
   * @param path column identifier
   * @param type type of the column
   * @param codec
   * @param encodings
   * @param firstDataPage
   * @param dictionaryPageOffset
   * @param valueCount
   * @param totalSize
   * @param totalUncompressedSize
   */
  IntColumnChunkMetaData(
      ColumnPath path, PrimitiveTypeName type, CompressionCodecName codec, Set<Encoding> encodings,
      long firstDataPage,
      long dictionaryPageOffset,
      long valueCount,
      long totalSize,
      long totalUncompressedSize) {
    super(ColumnChunkProperties.get(path, type, codec, encodings));
    this.firstDataPage = (int)firstDataPage;
    this.dictionaryPageOffset = (int)dictionaryPageOffset;
    this.valueCount = (int)valueCount;
    this.totalSize = (int)totalSize;
    this.totalUncompressedSize = (int)totalUncompressedSize;
  }

  /**
   * @return start of the column data offset
   */
  public long getFirstDataPageOffset() {
    return firstDataPage;
  }

  /**
   * @return the location of the dictionary page if any
   */
  public long getDictionaryPageOffset() {
    return dictionaryPageOffset;
  }

  /**
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
   * @return the totalSize
   */
  public long getTotalSize() {
    return totalSize;
  }

}
class LongColumnChunkMetaData extends ColumnChunkMetaData {

  private final long firstDataPage;
  private final long dictionaryPageOffset;
  private final long valueCount;
  private final long totalSize;
  private final long totalUncompressedSize;

  /**
   * @param path column identifier
   * @param type type of the column
   * @param codec
   * @param encodings
   * @param firstDataPage
   * @param dictionaryPageOffset
   * @param valueCount
   * @param totalSize
   * @param totalUncompressedSize
   */
  LongColumnChunkMetaData(
      ColumnPath path, PrimitiveTypeName type, CompressionCodecName codec, Set<Encoding> encodings,
      long firstDataPage,
      long dictionaryPageOffset,
      long valueCount,
      long totalSize,
      long totalUncompressedSize) {
    super(ColumnChunkProperties.get(path, type, codec, encodings));
    this.firstDataPage = firstDataPage;
    this.dictionaryPageOffset = dictionaryPageOffset;
    this.valueCount = valueCount;
    this.totalSize = totalSize;
    this.totalUncompressedSize = totalUncompressedSize;
  }

  /**
   * @return start of the column data offset
   */
  public long getFirstDataPageOffset() {
    return firstDataPage;
  }

  /**
   * @return the location of the dictionary page if any
   */
  public long getDictionaryPageOffset() {
    return dictionaryPageOffset;
  }

  /**
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
   * @return the totalSize
   */
  public long getTotalSize() {
    return totalSize;
  }

}