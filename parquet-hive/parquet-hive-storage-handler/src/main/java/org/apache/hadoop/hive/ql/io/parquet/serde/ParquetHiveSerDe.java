/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.serde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.parquet.io.api.Binary;

/**
 *
 * A ParquetHiveSerDe for Hive (with the deprecated package mapred)
 *
 */
public class ParquetHiveSerDe implements SerDe {

  public static final Text MAP_KEY = new Text("key");
  public static final Text MAP_VALUE = new Text("value");
  public static final Text MAP = new Text("map");
  public static final Text ARRAY = new Text("bag");

  private SerDeStats stats;
  private ObjectInspector objInspector;

  private enum LAST_OPERATION {
    SERIALIZE,
    DESERIALIZE,
    UNKNOWN
  }

  private LAST_OPERATION status;
  private long serializedSize;
  private long deserializedSize;

  @Override
  public final void initialize(final Configuration conf, final Properties tbl) throws SerDeException {

    final TypeInfo rowTypeInfo;
    final List<String> columnNames;
    final List<TypeInfo> columnTypes;
    // Get column names and sort order
    final String columnNameProperty = tbl.getProperty(IOConstants.COLUMNS);
    final String columnTypeProperty = tbl.getProperty(IOConstants.COLUMNS_TYPES);

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
      throw new IllegalArgumentException("ParquetHiveSerde initialization failed. Number of column " +
        "name and column type differs. columnNames = " + columnNames + ", columnTypes = " +
        columnTypes);
    }
    // Create row related objects
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    this.objInspector = new ArrayWritableObjectInspector((StructTypeInfo) rowTypeInfo);

    // Stats part
    stats = new SerDeStats();
    serializedSize = 0;
    deserializedSize = 0;
    status = LAST_OPERATION.UNKNOWN;
  }

  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    status = LAST_OPERATION.DESERIALIZE;
    deserializedSize = 0;
    if (blob instanceof ArrayWritable) {
      deserializedSize = ((ArrayWritable) blob).get().length;
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
    return ArrayWritable.class;
  }

  @Override
  public Writable serialize(final Object obj, final ObjectInspector objInspector)
      throws SerDeException {
    if (!objInspector.getCategory().equals(Category.STRUCT)) {
      throw new SerDeException("Cannot serialize " + objInspector.getCategory() + ". Can only serialize a struct");
    }
    final ArrayWritable serializeData = createStruct(obj, (StructObjectInspector) objInspector);
    serializedSize = serializeData.get().length;
    status = LAST_OPERATION.SERIALIZE;
    return serializeData;
  }

  private ArrayWritable createStruct(final Object obj, final StructObjectInspector inspector)
      throws SerDeException {
    final List<? extends StructField> fields = inspector.getAllStructFieldRefs();
    final Writable[] arr = new Writable[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      final StructField field = fields.get(i);
      final Object subObj = inspector.getStructFieldData(obj, field);
      final ObjectInspector subInspector = field.getFieldObjectInspector();
      arr[i] = createObject(subObj, subInspector);
    }
    return new ArrayWritable(Writable.class, arr);
  }

  private Writable createMap(final Object obj, final MapObjectInspector inspector)
      throws SerDeException {
    final Map<?, ?> sourceMap = inspector.getMap(obj);
    final ObjectInspector keyInspector = inspector.getMapKeyObjectInspector();
    final ObjectInspector valueInspector = inspector.getMapValueObjectInspector();
    final List<ArrayWritable> array = new ArrayList<ArrayWritable>();

    if (sourceMap != null) {
      for (final Entry<?, ?> keyValue : sourceMap.entrySet()) {
        final Writable key = createObject(keyValue.getKey(), keyInspector);
        final Writable value = createObject(keyValue.getValue(), valueInspector);
        if (key != null) {
          Writable[] arr = new Writable[2];
          arr[0] = key;
          arr[1] = value;
          array.add(new ArrayWritable(Writable.class, arr));
        }
      }
    }
    if (array.size() > 0) {
      final ArrayWritable subArray = new ArrayWritable(ArrayWritable.class,
          array.toArray(new ArrayWritable[array.size()]));
      return new ArrayWritable(Writable.class, new Writable[] {subArray});
    } else {
      return null;
    }
  }

  private ArrayWritable createArray(final Object obj, final ListObjectInspector inspector)
      throws SerDeException {
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
      final ArrayWritable subArray = new ArrayWritable(array.get(0).getClass(),
          array.toArray(new Writable[array.size()]));
      return new ArrayWritable(Writable.class, new Writable[] {subArray});
    } else {
      return null;
    }
  }

  private Writable createPrimitive(final Object obj, final PrimitiveObjectInspector inspector)
      throws SerDeException {
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
      return new ShortWritable((short) ((ShortObjectInspector) inspector).get(obj));
    case STRING:
      return new BinaryWritable(Binary.fromString(((StringObjectInspector) inspector).getPrimitiveJavaObject(obj)));
    default:
      throw new SerDeException("Unknown primitive : " + inspector.getPrimitiveCategory());
    }
  }

  private Writable createObject(final Object obj, final ObjectInspector inspector) throws SerDeException {
    switch (inspector.getCategory()) {
    case STRUCT:
      return createStruct(obj, (StructObjectInspector) inspector);
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
    // must be different
    assert (status != LAST_OPERATION.UNKNOWN);
    if (status == LAST_OPERATION.SERIALIZE) {
      stats.setRawDataSize(serializedSize);
    } else {
      stats.setRawDataSize(deserializedSize);
    }
    return stats;
  }
}
