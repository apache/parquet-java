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
package parquet.scrooge;

import com.twitter.scrooge.ThriftStructCodec;
import com.twitter.scrooge.ThriftStructField;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftTypeID;
import static parquet.thrift.struct.ThriftField.Requirement;
import static parquet.thrift.struct.ThriftField.Requirement.*;

import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.Manifest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Class to convert a scrooge generated class to {@link ThriftType.StructType}. {@link ScroogeReadSupport } uses this
 * class to get the requested schema
 *
 * @author Tianshuo Deng
 */
public class ScroogeStructConverter {

  /**
   * convert a given scrooge generated class to {@link ThriftType.StructType}
   *
   * @param scroogeClass
   * @return
   * @throws Exception
   */
  public ThriftType.StructType convert(Class scroogeClass)  {
    return convertStructFromClassName(scroogeClass.getName());
  }

  private ThriftType.StructType convertStructFromClassName(String className) {
    //Struct metadata is stored in the companion class
    Class<?> companionClass = null;
    try {
      companionClass = Class.forName(className + "$");
    } catch (ClassNotFoundException e) {
     throw new IllegalArgumentException("Can not find companion object for scrooge class " + className, e);
    }
    return convertCompanionClassToStruct(companionClass);
  }

  private ThriftType.StructType convertCompanionClassToStruct(Class<?> companionClass) {
    ThriftStructCodec companionObject = null;
    try {
      companionObject = (ThriftStructCodec<?>)companionClass.getField("MODULE$").get(null);
    } catch (Exception e) {
      throw new RuntimeException("Can not get ThriftStructCodec from companion object of " + companionClass.getName(), e);
    }

    List<ThriftField> children = new LinkedList<ThriftField>();//{@link ThriftType.StructType} uses foreach loop to iterate the children, yields O(n) time for linked list
    Iterable<ThriftStructField> scroogeFields = JavaConversions.asIterable(companionObject.metaData().fields());
    for (ThriftStructField field : scroogeFields) {
      children.add(toThriftField(field));
    }
    return new ThriftType.StructType(children);
  }

  /**
   * Convert thrift field in scrooge to ThriftField in parquet
   * Use reflection to detect if a field is optional or required since scrooge does not provide requirement information
   * in generated classes.
   * This will not correctly recognize fields that are not specified with a requirement type eg.
   * struct Address {
   * 1: string street
   * }
   * street will be identified as "REQUIRED"
   *
   * @param scroogeField
   * @return
   * @throws Exception
   */
  public ThriftField toThriftField(ThriftStructField scroogeField) {
    Requirement requirement = isOptional(scroogeField)? OPTIONAL : REQUIRED;

    String fieldName = scroogeField.tfield().name;
    short fieldId = scroogeField.tfield().id;
    byte thriftTypeByte = scroogeField.tfield().type;
    ThriftTypeID typeId = ThriftTypeID.fromByte(thriftTypeByte);
    ThriftType thriftType;
    switch (typeId) {
    case BOOL:
      thriftType = new ThriftType.BoolType();
      break;
    case BYTE:
      thriftType = new ThriftType.ByteType();
      break;
    case DOUBLE:
      thriftType = new ThriftType.DoubleType();
      break;
    case I16:
      thriftType = new ThriftType.I16Type();
      break;
    case I32:
      thriftType = new ThriftType.I32Type();
      break;
    case I64:
      thriftType = new ThriftType.I64Type();
      break;
    case STRING:
      thriftType = new ThriftType.StringType();
      break;
    case STRUCT:
      thriftType = convertStructTypeField(scroogeField);
      break;
    case MAP:
      thriftType = convertMapTypeField(scroogeField, requirement);
      break;
    case SET:
      thriftType = convertSetTypeField(scroogeField, requirement);
      break;
    case LIST:
      thriftType = convertListTypeField(scroogeField, requirement);
      break;
    case ENUM:
      thriftType = convertEnumTypeField(scroogeField);
      break;
    case STOP:
    case VOID:
    default:
      throw new IllegalArgumentException("can't convert type " + typeId);
    }
    return new ThriftField(fieldName, fieldId, requirement, thriftType);
  }

  /**
   * extract type arguments from a thrift field, such as the key and value type in a map
   *
   * @param field
   * @return
   */
  private List<Class> getTypeArguments(ThriftStructField field) {
    Iterator<Manifest> it = ((Manifest)field.manifest().get()).typeArguments().iterator();
    List<Class> types = new ArrayList<Class>();
    while (it.hasNext()) {
      types.add(it.next().erasure());
    }
    return types;
  }

  private ThriftType convertSetTypeField(ThriftStructField f, Requirement requirement) {
    List<Class> typeArguments = getTypeArguments(f);
    ThriftType elementType = convertClassToThriftType(typeArguments.get(0));
    //Set only has one sub-field as element field, therefore using hard-coded 1 as fieldId,
    //it's the same as the solution used in ElephantBird
    ThriftField elementField = generateFieldWithoutId(f.name(), requirement, elementType);
    return new ThriftType.SetType(elementField);
  }

  private ThriftType convertListTypeField(ThriftStructField f, Requirement requirement) {
    List<Class> typeArguments = getTypeArguments(f);
    ThriftType elementType = convertClassToThriftType(typeArguments.get(0));
    ThriftField elementField = generateFieldWithoutId(f.name(), requirement, elementType);
    return new ThriftType.ListType(elementField);
  }

  private ThriftType convertMapTypeField(ThriftStructField f, Requirement requirement) {
    List<Class> typeArguments = getTypeArguments(f);
    Class keyClass = typeArguments.get(0);
    ThriftType keyType = convertClassToThriftType(keyClass);
    Class valueClass = typeArguments.get(1);

    ThriftField keyField = generateFieldWithoutId(f.name() + "_map_key", requirement, keyType);
    ThriftType valueType = convertClassToThriftType(valueClass);
    ThriftField valueField = generateFieldWithoutId(f.name() + "_map_value", requirement, valueType);
    return new ThriftType.MapType(keyField, valueField);
  }

  /**
   * Generate artificial field, this kind of fields do not have a field ID.
   * To be consistent with the behavior in ElephantBird, here uses 1 as the field ID
   *
   * @param fieldName
   * @param requirement
   * @param thriftType
   * @return
   */
  private ThriftField generateFieldWithoutId(String fieldName, Requirement requirement, ThriftType thriftType) {
    return new ThriftField(fieldName, (short)1, requirement, thriftType);
  }

  /**
   * In composite types,  such as the type of the key in a map, since we use reflection to get the type class, this method
   * does conversion based on the class provided.
   *
   * @param typeClass
   * @return
   * @throws Exception
   */
  private ThriftType convertClassToThriftType(Class typeClass) {
    if (typeClass == boolean.class) {
      return new ThriftType.BoolType();
    } else if (typeClass == byte.class) {
      return new ThriftType.ByteType();
    } else if (typeClass == double.class) {
      return new ThriftType.DoubleType();
    } else if (typeClass == short.class) {
      return new ThriftType.I16Type();
    } else if (typeClass == int.class) {
      return new ThriftType.I32Type();
    } else if (typeClass == long.class) {
      return new ThriftType.I64Type();
    } else if (typeClass == String.class) {
      return new ThriftType.StringType();
    } else {
      return convertStructFromClassName(typeClass.getName());
    }
  }

  private ThriftType convertStructTypeField(ThriftStructField f) {
    Type structClassType = f.method().getReturnType();
    if (isOptional(f)) {
      structClassType = extractClassFromOption(f.method().getGenericReturnType());
    }
    return convertStructFromClassName(((Class)structClassType).getName());
  }

  private Type extractClassFromOption(Type genericReturnType) {
    return ((ParameterizedType)genericReturnType).getActualTypeArguments()[0];
  }

  private boolean isOptional(ThriftStructField f) {
    return f.method().getReturnType() == scala.Option.class;
  }

  /**
   * When define an enum in scrooge, each enum value is a subclass of the enum class, the enum class could be Operation$
   */
  private List getEnumList(String enumName) throws ClassNotFoundException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException, InvocationTargetException {
    enumName += "$";//In scala generated code, the actual class is ended with $
    Class companionObjectClass = Class.forName(enumName);
    Object cObject = companionObjectClass.getField("MODULE$").get(null);
    Method listMethod = companionObjectClass.getMethod("list", new Class[]{});
    Object result = listMethod.invoke(cObject, null);
    return JavaConversions.asJavaList((Seq)result);
  }

  public ThriftType convertEnumTypeField(ThriftStructField f) {
    List<ThriftType.EnumValue> enumValues = new ArrayList<ThriftType.EnumValue>();

    String enumName;
    if (isOptional(f)) {
      enumName = ((Class)extractClassFromOption(f.method().getGenericReturnType())).getName();
    } else {
      enumName = f.method().getReturnType().getName();
    }
    try {
      List enumCollection = getEnumList(enumName);
      for (Object enumObj : enumCollection) {
        ScroogeEnumDesc enumDesc = ScroogeEnumDesc.getEnumDesc(enumObj);
        //be compatible with thrift generated enum which have capitalized name
        enumValues.add(new ThriftType.EnumValue(enumDesc.id, enumDesc.name.replaceAll("([a-z])([A-Z])","$1_$2").toUpperCase()));
      }
      return new ThriftType.EnumType(enumValues);
    } catch (Exception e) {
      throw new IllegalArgumentException("Can not convert enum field " + f, e);
    }
  }

  private static class ScroogeEnumDesc {
    private int id;
    private String name;

    public static ScroogeEnumDesc getEnumDesc(Object rawScroogeEnum) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Class enumClass = rawScroogeEnum.getClass();
      Method valueMethod = enumClass.getMethod("value", new Class[]{});
      Method nameMethod = enumClass.getMethod("name", new Class[]{});
      ScroogeEnumDesc result = new ScroogeEnumDesc();
      result.id = (Integer)valueMethod.invoke(rawScroogeEnum, null);
      result.name = (String)nameMethod.invoke(rawScroogeEnum, null);
      return result;
    }
  }
}
