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
import java.util.Arrays;

import redelm.schema.PrimitiveType.Primitive;

/**
 * Column meta data for a block stored in the file footer and passed in the InputSplit
 * @author Julien Le Dem
 *
 */
public class ColumnMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  private long repetitionStart;
  private long definitionStart;
  private long dataStart;
  private long dataEnd;
  private final String[] path;
  private final Primitive type;
  private int valueCount;
  private long repetitionUncompressedLength;
  private long definitionUncompressedLength;
  private long dataUncompressedLength;

  /**
   *
   * @param path column identifier
   * @param type type of the column
   */
  public ColumnMetaData(String[] path, Primitive type) {
    this.path = path;
    this.type = type;
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
  public Primitive getType() {
    return type;
  }

  /**
   *
   * @param repetitionStart offset in the file where repetition levels data starts
   */
  public void setRepetitionStart(long repetitionStart) {
    this.repetitionStart = repetitionStart;
  }

  /**
   *
   * @param definitionStart offset in the file where definition levels data starts
   */
  public void setDefinitionStart(long definitionStart) {
    this.definitionStart = definitionStart;
  }

  /**
   *
   * @param dataStart offset in the file where data starts
   */
  public void setDataStart(long dataStart) {
    this.dataStart = dataStart;
  }

  /**
   * @param dataEnd end of the column data
   */
  public void setDataEnd(long dataEnd) {
    this.dataEnd = dataEnd;
  }

  /**
   *
   * @return offset where the column data starts
   */
  public long getColumnStart() {
    return repetitionStart;
  }

 /**
  *
  * @return offset where repetition levels start
  */
  public long getRepetitionStart() {
    return repetitionStart;
  }

  /**
   *
   * @return offset where repetition levels end
   */
  public long getRepetitionEnd() {
    return definitionStart;
  }

  /**
   *
   * @return length of repetition level data
   */
  public long getRepetitionLength() {
    return getRepetitionEnd() - getRepetitionStart();
  }

  /**
   *
   * @return offset where definition levels start
   */
  public long getDefinitionStart() {
    return definitionStart;
  }

  /**
   *
   * @return offset where definition levels end
   */
  public long getDefinitionEnd() {
    return dataStart;
  }

  /**
   *
   * @return length of definition level data
   */
  public long getDefinitionLength() {
    return getDefinitionEnd() - getDefinitionStart();
  }

  /**
   *
   * @return start of the column data offset
   */
  public long getDataStart() {
    return dataStart;
  }

  /**
   *
   * @return offset of the end of the column data
   */
  public long getDataEnd() {
    return dataEnd;
  }

  /**
   *
   * @return length of the column data
   */
  public long getDataLength() {
    return getDataEnd() - getDataStart();
  }

  /**
   *
   * @return offset where column ends
   */
  public long getColumnEnd() {
    return dataEnd;
  }

  /**
   *
   * @return total length of the column for this block
   */
  public long getColumnLength() {
    return getColumnEnd() - getColumnStart();
  }

  /**
   *
   * @param valueCount count of values in this block of the column
   */
  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
  }

  /**
   *
   * @return count of values in this block of the column
   */
  public int getValueCount() {
    return valueCount;
  }

  /**
   *
   * @return uncompressed length of repetition levels
   */
  public long getRepetitionUncompressedLength() {
    return repetitionUncompressedLength;
  }

  /**
   *
   * @param repetitionUncompressedLength  uncompressed length of repetition levels
   */
  public void setRepetitionUncompressedLength(long repetitionUncompressedLength) {
    this.repetitionUncompressedLength = repetitionUncompressedLength;
  }

  /**
   *
   * @return uncompressed length of definition levels
   */
  public long getDefinitionUncompressedLength() {
    return definitionUncompressedLength;
  }

  /**
   *
   * @param definitionUncompressedLength uncompressed length of definition levels
   */
  public void setDefinitionUncompressedLength(long definitionUncompressedLength) {
    this.definitionUncompressedLength = definitionUncompressedLength;
  }

  /**
   *
   * @return  uncompressed length of data
   */
  public long getDataUncompressedLength() {
    return dataUncompressedLength;
  }

  /**
   *
   * @param dataUncompressedLength uncompressed length of data
   */
  public void setDataUncompressedLength(long dataUncompressedLength) {
    this.dataUncompressedLength = dataUncompressedLength;
  }

  @Override
  public String toString() {
    return "ColumnMetaData{" + repetitionStart + ", " + definitionStart + ", " + dataStart + ", " + dataEnd + " " + Arrays.toString(path) + "}";
  }

}
