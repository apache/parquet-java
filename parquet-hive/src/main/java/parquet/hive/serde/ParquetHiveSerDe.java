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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import parquet.hive.writable.BinaryWritable;

/**
 *
 * A ParquetHiveSerDe for Hive (with the deprecated package mapred)
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 *
 */
public class ParquetHiveSerDe implements SerDe {

  private List<String> columnNames;
  private List<TypeInfo> columnTypes;

  public static Text MAP_KEY = new Text("key");
  public static Text MAP_VALUE = new Text("value");
  public static Text MAP = new Text("map");
  public static Text ARRAY = new Text("bag");

  ObjectInspector objInspector;

  static final Log LOG = LogFactory.getLog(ParquetHiveSerDe.class);

  @Override
  final public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {

    final TypeInfo rowTypeInfo;

    // Get column names and sort order
    final String columnNameProperty = tbl.getProperty("columns");
    final String columnTypeProperty = tbl.getProperty("columns.types");

    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }

    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    if (columnNames.size() != columnTypes.size()) {
      throw new RuntimeException("ParquetHiveSerde initialization failed. Number of column name and column type differs.");
    }

    // Create row related objects
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);

    this.objInspector = new MapWritableObjectInspector((StructTypeInfo) rowTypeInfo);
  }

  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    if (blob instanceof MapWritable) {
      return blob;
    } else {
      return null;
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return MapWritable.class;
  }

  @Override
  public Writable serialize(final Object obj, final ObjectInspector objInspector) throws SerDeException {
    if (!objInspector.getCategory().equals(Category.STRUCT)) {
      throw new SerDeException("Can only serialize a struct");
    }

    return createStruct(obj, (StructObjectInspector) objInspector, columnNames);
  }


  private MapWritable createStruct(final Object obj, final StructObjectInspector inspector, final List<String> colNames) throws SerDeException {
    final MapWritable result = new MapWritable();
    final List<? extends StructField> fields = inspector.getAllStructFieldRefs();

    int i = 0;

    for (final StructField field : fields) {


      final Object subObj = inspector.getStructFieldData(obj, field);
      final ObjectInspector subInspector = field.getFieldObjectInspector();

      final Writable subResult = createObject(subObj, subInspector);

      // for the 1st lvl, the field names are "_col0" ... and we want the
      // real names
      final String colName = (colNames != null) ? colNames.get(i++) : field.getFieldName();
      if (subResult != null) {
        result.put(new Text(colName), subResult);
      }
    }

    return result;

  }

  private Writable createMap(final Object obj, final MapObjectInspector inspector) throws SerDeException {
    final Map<?, ?> sourceMap = inspector.getMap(obj);
    final ObjectInspector keyInspector = inspector.getMapKeyObjectInspector();
    final ObjectInspector valueInspector = inspector.getMapValueObjectInspector();
    final List<MapWritable> array = new ArrayList<MapWritable>();

    if (sourceMap != null) {
      for (final Entry<?, ?> keyValue : sourceMap.entrySet()) {
        final Writable key = createObject(keyValue.getKey(), keyInspector);
        final Writable value = createObject(keyValue.getValue(), valueInspector);

        if (key != null) {
          final MapWritable keyValueWritable = new MapWritable();
          keyValueWritable.put(MAP_KEY, key);
          keyValueWritable.put(MAP_VALUE, value);
          array.add(keyValueWritable);
        }

      }
    }

    if (array.size() > 0) {
      final ArrayWritable subArray = new ArrayWritable(MapWritable.class, array.toArray(new MapWritable[array.size()]));
      final MapWritable map = new MapWritable();
      map.put(MAP, subArray);
      return map;
    } else {
      return null;
    }
  }

  private MapWritable createArray(final Object obj, final ListObjectInspector inspector) throws SerDeException {
    final List<?> sourceArray = inspector.getList(obj);
    final ObjectInspector subInspector = inspector.getListElementObjectInspector();
    final List<Writable> array = new ArrayList<Writable>();

    if (sourceArray != null) {
      for (final Object curObj : sourceArray) {
        final Writable newObj = createObject(curObj, subInspector);
        if (newObj != null) {
          array.add(newObj);
        }
      }
    }

    if (array.size() > 0) {
      final ArrayWritable subArray = new ArrayWritable(array.get(0).getClass(), array.toArray(new Writable[array.size()]));
      final MapWritable map = new MapWritable();
      map.put(ARRAY, subArray);
      return map;
    } else {
      return null;
    }
  }

  private Writable createPrimitive(final Object obj, final PrimitiveObjectInspector inspector) throws SerDeException {

    if (obj == null) {
      return null;
    }

    switch (inspector.getPrimitiveCategory()) {
    case VOID:
      return null;
    case BOOLEAN:
      return new BooleanWritable(((BooleanObjectInspector) inspector).get(obj) ? Boolean.TRUE : Boolean.FALSE);
    case BYTE:
      return new ByteWritable((byte) ((ByteObjectInspector) inspector).get(obj));
    case DOUBLE:
      return new DoubleWritable(((DoubleObjectInspector) inspector).get(obj));
    case FLOAT:
      return new FloatWritable(((FloatObjectInspector) inspector).get(obj));
    case INT:
      return new IntWritable(((IntObjectInspector) inspector).get(obj));
    case LONG:
      return new LongWritable(((LongObjectInspector) inspector).get(obj));
    case SHORT:
      return new ByteWritable((byte) ((ShortObjectInspector) inspector).get(obj));
    case STRING:
      return new BinaryWritable(((StringObjectInspector) inspector).getPrimitiveJavaObject(obj));
    default:
      throw new SerDeException("Unknown primitive");
    }
  }

  private Writable createObject(final Object obj, final ObjectInspector inspector) throws SerDeException {
    switch (inspector.getCategory()) {
    case STRUCT:
      return createStruct(obj, (StructObjectInspector) inspector, null);
    case LIST:
      return createArray(obj, (ListObjectInspector) inspector);
    case MAP:
      return createMap(obj, (MapObjectInspector) inspector);
    case PRIMITIVE:
      return createPrimitive(obj, (PrimitiveObjectInspector) inspector);
    default:
      throw new SerDeException("Unknown data type" + inspector.getCategory());
    }
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }
}
