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

import parquet.schema.GroupType;

import java.util.ArrayList;
import java.util.List;

public class GroupColumnIO2<T extends GroupType> extends ColumnIO2<T> {
  private final List<ColumnIO2<?>> children;

  public GroupColumnIO2(
      final T type,
      final String name,
      final LeafInfo leafInfo,
      final List<ColumnIO2<?>> children) {
    super(type, name, leafInfo);
    this.children = children;
  }

  @Override
  void setParent(final ColumnIO2<?> parent) {
    super.setParent(parent);
    for (final ColumnIO2<?> child : children) {
      child.setParent(this);
    }
  }

  @Override
  PrimitiveColumnIO2 getLast() {
    return children.get(children.size() - 1).getLast();
  }

  @Override
  PrimitiveColumnIO2 getFirst() {
    return children.get(0).getFirst();
  }

  public List<ColumnIO2<?>> getChildren() {
    return children;
  }

  @Override
  public final List<PrimitiveColumnIO2> getLeafColumnIO() {
    final ArrayList<PrimitiveColumnIO2> arr = new ArrayList<PrimitiveColumnIO2>();
    for (final ColumnIO2<?> child : children) {
      arr.addAll(child.getLeafColumnIO());
    }
    return arr;
  }
}
