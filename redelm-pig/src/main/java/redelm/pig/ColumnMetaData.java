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
package redelm.pig;

import java.io.Serializable;
import java.util.Arrays;

import redelm.schema.PrimitiveType.Primitive;

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

  public ColumnMetaData(String[] path, Primitive type) {
    this.path = path;
    this.type = type;
  }

  public String[] getPath() {
    return path;
  }

  public Primitive getType() {
    return type;
  }

  public void setRepetitionStart(long repetitionStart) {
    this.repetitionStart = repetitionStart;
  }

  public void setDefinitionStart(long definitionStart) {
    this.definitionStart = definitionStart;
  }

  public void setDataStart(long dataStart) {
    this.dataStart = dataStart;
  }

  public void setDataEnd(long dataEnd) {
    this.dataEnd = dataEnd;
  }

  public long getColumnStart() {
    return repetitionStart;
  }

  public long getRepetitionStart() {
    return repetitionStart;
  }

  public long getRepetitionEnd() {
    return definitionStart;
  }

  public long getRepetitionLength() {
    return getRepetitionEnd() - getRepetitionStart();
  }

  public long getDefinitionStart() {
    return definitionStart;
  }

  public long getDefinitionEnd() {
    return dataStart;
  }

  public long getDefinitionLength() {
    return getDefinitionEnd() - getDefinitionStart();
  }

  public long getDataStart() {
    return dataStart;
  }

  public long getDataEnd() {
    return dataEnd;
  }

  public long getDataLength() {
    return getDataEnd() - getDataStart();
  }

  public long getColumnEnd() {
    return dataEnd;
  }

  public long getColumnLength() {
    return getColumnEnd() - getColumnStart();
  }

  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
  }

  public int getValueCount() {
    return valueCount;
  }

  public void setRepetitionUncompressedLenght(long repetitionUncompressedLength) {
    this.repetitionUncompressedLength = repetitionUncompressedLength;
  }

  public long getRepetitionUncompressedLength() {
    return repetitionUncompressedLength;
  }

  public void setRepetitionUncompressedLength(long repetitionUncompressedLength) {
    this.repetitionUncompressedLength = repetitionUncompressedLength;
  }

  public long getDefinitionUncompressedLength() {
    return definitionUncompressedLength;
  }

  public void setDefinitionUncompressedLength(long definitionUncompressedLength) {
    this.definitionUncompressedLength = definitionUncompressedLength;
  }

  public long getDataUncompressedLength() {
    return dataUncompressedLength;
  }

  public void setDataUncompressedLength(long dataUncompressedLength) {
    this.dataUncompressedLength = dataUncompressedLength;
  }

  @Override
  public String toString() {
    return "ColumnMetaData{" + repetitionStart + ", " + definitionStart + ", " + dataStart + ", " + dataEnd + " " + Arrays.toString(path) + "}";
  }

}
