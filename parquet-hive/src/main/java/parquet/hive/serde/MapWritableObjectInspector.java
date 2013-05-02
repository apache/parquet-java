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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import parquet.hive.writable.BinaryWritable;


/**
 *
 * A MapWritableObjectInspector for Hive (with the deprecated package mapred)
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 *
 */
public class MapWritableObjectInspector extends StructObjectInspector {
  private final TypeInfo typeInfo;
  private final List<TypeInfo> fieldInfos;
  private final List<String> fieldNames;
  private final List<StructField> fields;
  private final HashMap<String, StructFieldImpl> fieldsByName;
  public MapWritableObjectInspector(final StructTypeInfo rowTypeInfo) {

    typeInfo = rowTypeInfo;
    fieldNames = rowTypeInfo.getAllStructFieldNames();
    fieldInfos = rowTypeInfo.getAllStructFieldTypeInfos();
    fields = new ArrayList<StructField>(fieldNames.size());
    fieldsByName = new HashMap<String, StructFieldImpl>();

    for (int i = 0; i < fieldNames.size(); ++i) {
      final String name = fieldNames.get(i);
      final TypeInfo fieldInfo = fieldInfos.get(i);

      final StructFieldImpl field = new StructFieldImpl(name, getObjectInspector(fieldInfo));
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
      return new MapWritableObjectInspector((StructTypeInfo) typeInfo);
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
      return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.shortTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
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
    if (data != null && data instanceof MapWritable) {
      final MapWritable arr = (MapWritable) data;
      final Text fieldName = new Text(((StructFieldImpl) fieldRef).getFieldName());
      return arr.get(fieldName);
    }
    return null;
  }

  @Override
  public StructField getStructFieldRef(final String name) {
    return fieldsByName.get(name);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(final Object data) {
    ArrayList<Object> result = null;

    if (data != null && data instanceof MapWritable) {
      final MapWritable arr = (MapWritable) data;
      result = new ArrayList<Object>(fieldNames.size());

      for (final String field : fieldNames) {
        result.add(arr.get(new Text(field)));
      }
    }
    return result;
  }

  class StructFieldImpl implements StructField {

    private final String name;
    private final ObjectInspector inspector;

    public StructFieldImpl(final String name, final ObjectInspector inspector) {
      this.name = name;
      this.inspector = inspector;
    }

    @Override
    public String getFieldComment() {
      return "";
    }

    @Override
    public String getFieldName() {
      return name;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }
  }

  /**
   * A JavaStringObjectInspector inspects a Java String Object.
   */
  public class JavaStringBinaryObjectInspector extends AbstractPrimitiveJavaObjectInspector implements StringObjectInspector {

    JavaStringBinaryObjectInspector() {
      super(PrimitiveObjectInspectorUtils.stringTypeEntry);
    }

    @Override
    public Text getPrimitiveWritableObject(final Object o) {
      return o == null ? null : new Text(((BinaryWritable) o).getBytes());
    }

    @Override
    public String getPrimitiveJavaObject(final Object o) {
      try {
        return new String(((BinaryWritable) o).getBytes(), "UTF-8");
      } catch (final UnsupportedEncodingException e) {
        return null;
      }
    }

  }
}
