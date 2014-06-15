/**
 * Copyright 2014 GoDaddy, Inc.
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
package parquet.io2;

import parquet.column.ColumnDescriptor;
import parquet.schema.Type;

import java.util.Arrays;

final class ColumnPath {
  private final Type type;
  private final String[] namedPath;
  private final int[] indexPath;
  private final int index;
  private final int repetitionLevel;
  private final int definitionLevel;

  public final static ColumnPath empty = new ColumnPath(null, new String[0], new int[0], 0, 0, 0);

  ColumnPath(
      final Type type,
      final int index) {
    this(
        type,
        index,
        type.isRepetition(Type.Repetition.REPEATED) ? 1 : 0,
        type.isRepetition(Type.Repetition.REQUIRED) ? 0 : 1);
  }

  ColumnPath(
      final Type type,
      final int index,
      final int repetitionLevel,
      final int definitionLevel) {
    this(
        type,
        new String[]{type.getName()},
        new int[]{index},
        index,
        repetitionLevel,
        definitionLevel);
  }

  private ColumnPath(
      final Type type,
      final String[] namedPath,
      final int[] indexPath,
      final int index,
      final int repetitionLevel,
      final int definitionLevel) {
    this.type = type;
    this.namedPath = namedPath;
    this.indexPath = indexPath;
    this.index = index;
    this.repetitionLevel = repetitionLevel;
    this.definitionLevel = definitionLevel;
  }

  public ColumnDescriptor getColumnDescriptor() {
    return new ColumnDescriptor(
        namedPath,
        type.asPrimitiveType().getPrimitiveTypeName(),
        type.asPrimitiveType().getTypeLength(),
        repetitionLevel,
        definitionLevel);
  }

  public String[] getPath() {
    return namedPath;
  }

  public int[] getIndexes() {
    return indexPath;
  }

  public ColumnPath combine(final ColumnPath other) {
    String[] newNamedPath = Arrays.copyOf(this.namedPath, this.namedPath.length + other.namedPath.length);
    System.arraycopy(other.namedPath, 0, newNamedPath, this.namedPath.length, other.namedPath.length);

    int[] newIndexPath = Arrays.copyOf(this.indexPath, this.indexPath.length + other.indexPath.length);
    System.arraycopy(other.indexPath, 0, newIndexPath, this.indexPath.length, other.indexPath.length);

    return new ColumnPath(
        other.type,
        newNamedPath,
        newIndexPath,
        other.index,
        this.repetitionLevel + other.repetitionLevel,
        this.definitionLevel + other.definitionLevel);
  }

  public int getRepetitionLevel() {
    return repetitionLevel;
  }

  public int getDefinitionLevel() {
    return definitionLevel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ColumnPath that = (ColumnPath) o;

    if (definitionLevel != that.definitionLevel) return false;
    if (index != that.index) return false;
    if (repetitionLevel != that.repetitionLevel) return false;
    if (!Arrays.equals(indexPath, that.indexPath)) return false;
    if (!Arrays.equals(namedPath, that.namedPath)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(namedPath);
    result = 31 * result + Arrays.hashCode(indexPath);
    result = 31 * result + index;
    result = 31 * result + repetitionLevel;
    result = 31 * result + definitionLevel;
    return result;
  }

  @Override
  public String toString() {
    return "r:"+getRepetitionLevel()
        +" d:"+getDefinitionLevel()
        +" "+ Arrays.toString(namedPath);
  }
}
