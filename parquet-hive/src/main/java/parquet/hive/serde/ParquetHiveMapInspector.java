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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class ParquetHiveMapInspector implements MapObjectInspector {

  private final ObjectInspector keyInspector;
  private final ObjectInspector valueInspector;

  public ParquetHiveMapInspector(final ObjectInspector keyInspector, final ObjectInspector valueInspector) {
    this.keyInspector = keyInspector;
    this.valueInspector = valueInspector;
  }

  @Override
  public String getTypeName() {
    return "map<" + keyInspector.getTypeName() + "," + valueInspector.getTypeName() + ">";
  }

  @Override
  public Category getCategory() {
    return Category.MAP;
  }

  @Override
  public ObjectInspector getMapKeyObjectInspector() {
    return keyInspector;
  }

  @Override
  public ObjectInspector getMapValueObjectInspector() {
    return valueInspector;
  }

  @Override
  public Object getMapValueElement(final Object data, final Object key) {

    if (data == null) {
      return null;
    }

    final Writable[] mapContainer = ((ArrayWritable) data).get();
    final Writable[] mapArray = ((ArrayWritable) mapContainer[0]).get();

    for (final Writable obj : mapArray) {
      final ArrayWritable mapObj = (ArrayWritable) obj;
      final Writable[] arr = mapObj.get();
      if (arr[0] == key) {
        return arr[1];
      }
    }

    return null;
  }

  @Override
  public Map<?, ?> getMap(final Object data) {

    if (data == null) {
      return null;
    }

    final Writable[] mapContainer = ((ArrayWritable) data).get();
    final Writable[] mapArray = ((ArrayWritable) mapContainer[0]).get();
    final Map<Writable, Writable> map = new HashMap<Writable, Writable>();

    for (final Writable obj : mapArray) {
      final ArrayWritable mapObj = (ArrayWritable) obj;
      final Writable[] arr = mapObj.get();
      map.put(arr[0], arr[1]);
    }

    return map;
  }

  @Override
  public int getMapSize(final Object data) {
    final Writable[] mapContainer = ((ArrayWritable) data).get();
    return ((ArrayWritable) mapContainer[0]).get().length;
  }
}
