/**
 * Copyright 2013 Criteo.
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
package parquet.hive.serde;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class ParquetHiveArrayInspector implements SettableListObjectInspector {

  ObjectInspector arrayElementInspector;

  public ParquetHiveArrayInspector(final ObjectInspector arrayElementInspector) {
    this.arrayElementInspector = arrayElementInspector;
  }

  @Override
  public String getTypeName() {
    return "array<" + arrayElementInspector.getTypeName() + ">";
  }

  @Override
  public Category getCategory() {
    return Category.LIST;
  }

  @Override
  public ObjectInspector getListElementObjectInspector() {
    return arrayElementInspector;
  }

  @Override
  public Object getListElement(final Object data, final int index) {
    if (data == null) {
      return null;
    }

    final Writable subObj = ((ArrayWritable) data).get()[0];

    if (subObj == null) {
      return null;
    }

    return ((ArrayWritable) subObj).get()[index];
  }

  @Override
  public int getListLength(final Object data) {
    if (data == null) {
      return 0;
    }

    final Writable subObj = ((ArrayWritable) data).get()[0];

    if (subObj == null) {
      return 0;
    }

    return ((ArrayWritable) subObj).get().length;
  }

  @Override
  public List<?> getList(final Object data) {
    if (data == null) {
      return null;
    }

    final Writable subObj = ((ArrayWritable) data).get()[0];

    if (subObj == null) {
      return null;
    }

    final Writable[] array = ((ArrayWritable) subObj).get();
    final List<Writable> list = new ArrayList<Writable>();

    for (final Writable obj : array) {
      list.add(obj);
    }

    return list;
  }

  @Override
  public Object create(int size) {
    ArrayList<Object> result = new ArrayList<Object>(size);
    for (int i = 0; i < size; ++i) {
      result.add(null);
    }
    return result;
  }

  @Override
  public Object set(Object list, int index, Object element) {
    List l = (List) list;
    for (int i = l.size(); i < index + 1; ++i) {
      l.add(null);
    }
    l.set(index, element);
    return list;
  }

  @Override
  public Object resize(Object list, int newSize) {
    ((ArrayList) list).ensureCapacity(newSize);
    return list;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || o.getClass() != getClass()) {
      return false;
    } else if (o == this) {
      return true;
    } else {
      ObjectInspector other = ((ParquetHiveArrayInspector) o).arrayElementInspector;
      return other.equals(arrayElementInspector);
    }
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 29 * hash + (this.arrayElementInspector != null ? this.arrayElementInspector.hashCode() : 0);
    return hash;
  }
}
