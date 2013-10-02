/**
 * Copyright 2013 Criteo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package parquet.hive.serde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import parquet.hive.writable.BinaryWritable;
import parquet.io.api.Binary;

/**
 *
 * An ArrayWritableObjectInspector for Hive (with the deprecated package mapred)
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 *
 */
public class ArrayWritableObjectInspector extends SettableStructObjectInspector {

  private final TypeInfo typeInfo;
  private final List<TypeInfo> fieldInfos;
  private final List<String> fieldNames;
  private final List<StructField> fields;
  private final HashMap<String, StructFieldImpl> fieldsByName;

  public ArrayWritableObjectInspector(final StructTypeInfo rowTypeInfo) {

    typeInfo = rowTypeInfo;
    fieldNames = rowTypeInfo.getAllStructFieldNames();
    fieldInfos = rowTypeInfo.getAllStructFieldTypeInfos();
    fields = new ArrayList<StructField>(fieldNames.size());
    fieldsByName = new HashMap<String, StructFieldImpl>();

    for (int i = 0; i < fieldNames.size(); ++i) {
      final String name = fieldNames.get(i);
      final TypeInfo fieldInfo = fieldInfos.get(i);

      final StructFieldImpl field = new StructFieldImpl(name, getObjectInspector(fieldInfo), i);
      fields.add(field);
      fieldsByName.put(name, field);
    }
  }

  private ObjectInspector getObjectInspector(final TypeInfo typeInfo) {
    if (typeInfo.equals(TypeInfoFactory.doubleTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.booleanTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.floatTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.intTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.longTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.stringTypeInfo)) {
      return new JavaStringBinaryObjectInspector();
    } else if (typeInfo.getCategory().equals(Category.STRUCT)) {
      return new ArrayWritableObjectInspector((StructTypeInfo) typeInfo);
    } else if (typeInfo.getCategory().equals(Category.LIST)) {
      final TypeInfo subTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
      return new ParquetHiveArrayInspector(getObjectInspector(subTypeInfo));
    } else if (typeInfo.getCategory().equals(Category.MAP)) {
      final TypeInfo keyTypeInfo = ((MapTypeInfo) typeInfo).getMapKeyTypeInfo();
      final TypeInfo valueTypeInfo = ((MapTypeInfo) typeInfo).getMapValueTypeInfo();
      return new ParquetHiveMapInspector(getObjectInspector(keyTypeInfo), getObjectInspector(valueTypeInfo));
    } else if (typeInfo.equals(TypeInfoFactory.timestampTypeInfo)) {
      throw new NotImplementedException("timestamp not implemented yet");
    } else if (typeInfo.equals(TypeInfoFactory.byteTypeInfo)) {
      return new ParquetByteInspector();
    } else if (typeInfo.equals(TypeInfoFactory.shortTypeInfo)) {
      return new ParquetShortInspector();
    } else {
      throw new RuntimeException("Unknown field info: " + typeInfo);
    }

  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public String getTypeName() {
    return typeInfo.getTypeName();
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  @Override
  public Object getStructFieldData(final Object data, final StructField fieldRef) {
    if (data == null) {
      return null;
    }

    if (data instanceof ArrayWritable) {
      final ArrayWritable arr = (ArrayWritable) data;
      return arr.get()[((StructFieldImpl) fieldRef).getIndex()];
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }

  @Override
  public StructField getStructFieldRef(final String name) {
    return fieldsByName.get(name);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(final Object data) {
    if (data == null) {
      return null;
    }

    if (data instanceof ArrayWritable) {
      final ArrayWritable arr = (ArrayWritable) data;
      final Object[] arrWritable = arr.get();
      return new ArrayList<Object>(Arrays.asList(arrWritable));
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }

  @Override
  public Object create() {
    final ArrayList<Object> list = new ArrayList<Object>(fields.size());
    for (int i = 0; i < fields.size(); ++i) {
      list.add(null);
    }
    return list;
  }

  @Override
  public Object setStructFieldData(Object struct, StructField field, Object fieldValue) {
    final ArrayList<Object> list = (ArrayList<Object>) struct;
    list.set(((StructFieldImpl) field).getIndex(), fieldValue);
    return list;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ArrayWritableObjectInspector other = (ArrayWritableObjectInspector) obj;
    if (this.typeInfo != other.typeInfo && (this.typeInfo == null || !this.typeInfo.equals(other.typeInfo))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 29 * hash + (this.typeInfo != null ? this.typeInfo.hashCode() : 0);
    return hash;
  }

  class StructFieldImpl implements StructField {

    private final String name;
    private final ObjectInspector inspector;
    private final int index;

    public StructFieldImpl(final String name, final ObjectInspector inspector, final int index) {
      this.name = name;
      this.inspector = inspector;
      this.index = index;
    }

    @Override
    public String getFieldComment() {
      return "";
    }

    @Override
    public String getFieldName() {
      return name;
    }

    public int getIndex() {
      return index;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }
  }

  public class ParquetShortInspector
          extends AbstractPrimitiveJavaObjectInspector
          implements SettableShortObjectInspector {

    ParquetShortInspector() {
      super(PrimitiveObjectInspectorUtils.shortTypeEntry);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
      return o == null ? null : new ShortWritable(get(o));
    }

    @Override
    public Object create(final short val) {
      return new ShortWritable(val);
    }

    @Override
    public Object set(final Object o, final short val) {
      ((ShortWritable) o).set(val);
      return o;
    }

    @Override
    public short get(Object o) {
      // Accept int writables and convert them.
      if (o instanceof IntWritable) {
        return (short) ((IntWritable) o).get();
      }
      return ((ShortWritable) o).get();
    }
  }

  public class ParquetByteInspector
          extends AbstractPrimitiveJavaObjectInspector
          implements SettableByteObjectInspector {

    ParquetByteInspector() {
      super(PrimitiveObjectInspectorUtils.byteTypeEntry);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
      return o == null ? null : new ByteWritable(get(o));
    }

    @Override
    public Object create(final byte val) {
      return new ByteWritable(val);
    }

    @Override
    public Object set(final Object o, final byte val) {
      ((ByteWritable) o).set(val);
      return o;
    }

    @Override
    public byte get(Object o) {
      // Accept int writables and convert them.
      if (o instanceof IntWritable) {
        return (byte) ((IntWritable) o).get();
      }
      return ((ByteWritable) o).get();
    }
  }

  /**
   * A JavaStringObjectInspector inspects a Java String Object.
   */
  public class JavaStringBinaryObjectInspector extends AbstractPrimitiveJavaObjectInspector implements SettableStringObjectInspector {

    JavaStringBinaryObjectInspector() {
      super(PrimitiveObjectInspectorUtils.stringTypeEntry);
    }

    @Override
    public Text getPrimitiveWritableObject(final Object o) {
      return o == null ? null : new Text(((BinaryWritable) o).getBytes());
    }

    @Override
    public String getPrimitiveJavaObject(final Object o) {
      return ((BinaryWritable) o).getString();
    }

    @Override
    public Object set(final Object o, final Text text) {
      return new BinaryWritable(text == null ? null : Binary.fromByteArray(text.getBytes()));
    }

    @Override
    public Object set(final Object o, final String string) {
      return new BinaryWritable(string == null ? null : Binary.fromString(string));
    }

    @Override
    public Object create(final Text text) {
      if (text == null) {
        return null;
      }
      return text.toString();
    }

    @Override
    public Object create(final String string) {
      return string;
    }
  }
}
