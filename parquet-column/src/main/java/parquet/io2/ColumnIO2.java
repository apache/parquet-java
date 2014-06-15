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

import parquet.io.InvalidRecordException;
import parquet.schema.Type;

import java.util.List;

abstract class ColumnIO2<T extends Type> {
  private final T type;
  private final String name;
  private final LeafInfo leafInfo;
  private ColumnIO2<?> parent;

  public ColumnIO2(
      final T type,
      final String name,
      final LeafInfo leafInfo) {
    this.type = type;
    this.name = name;
    this.leafInfo = leafInfo;
  }

  public final T getType() {
    return type;
  }

  public final String getName() {
    return name;
  }

  public final LeafInfo getLeafInfo() {
    return leafInfo;
  }

  public abstract List<PrimitiveColumnIO2> getLeafColumnIO();

  void setParent(final ColumnIO2<?> parent) {
    this.parent = parent;
  }

  ColumnIO2<?> getParent() {
    return parent;
  }

  abstract PrimitiveColumnIO2 getLast();
  abstract PrimitiveColumnIO2 getFirst();

  boolean isFirst() {
    return getFirst() == this;
  }

  boolean isLast() {
    return getLast() == this;
  }

  ColumnIO2<?> getParent(int r) {
    if (leafInfo.getLogicalPath().getRepetitionLevel() == r && getType().isRepetition(Type.Repetition.REPEATED)) {
      return this;
    } else if (parent != null && parent.leafInfo.getLogicalPath().getDefinitionLevel() >= r) {
      return getParent().getParent(r);
    } else {
      throw new InvalidRecordException("no parent("+r+") for "+this);
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName()+" "+type.getName()
        +" "+leafInfo.getLogicalPath();
  }
}
