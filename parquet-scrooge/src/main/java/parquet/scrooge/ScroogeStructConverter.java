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
package parquet.scrooge;

import com.twitter.scrooge.ThriftStructCodec;
import com.twitter.scrooge.ThriftStructFieldInfo;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftType.StructType.StructOrUnionType;
import parquet.thrift.struct.ThriftTypeID;
import scala.collection.JavaConversions;
import scala.collection.JavaConversions$;
import scala.collection.Seq;
import scala.reflect.Manifest;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static parquet.thrift.struct.ThriftField.Requirement;
import static parquet.thrift.struct.ThriftField.Requirement.*;

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
  public ThriftType.StructType convert(Class scroogeClass) {
    return convertStructFromClass(scroogeClass);
  }

  private Class getCompanionClass(Class klass) {
    try {
     return Class.forName(klass.getName() + "$");
    } catch (ClassNotFoundException e) {
      throw new ScroogeSchemaConversionException("Can not find companion object for scrooge class " + klass, e);
    }
  }

  private ThriftType.StructType convertStructFromClass(Class klass) {
    return convertCompanionClassToStruct(getCompanionClass(klass));
  }

  private ThriftType.StructType convertCompanionClassToStruct(Class<?> companionClass) {
    ThriftStructCodec<?> companionObject;
    try {
      companionObject = (ThriftStructCodec<?>)companionClass.getField("MODULE$").get(null);
    } catch (ReflectiveOperationException e) {
      throw new ScroogeSchemaConversionException("Can not get ThriftStructCodec from companion object of " + companionClass.getName(), e);
    }

    List<ThriftField> children = new LinkedList<ThriftField>();//{@link ThriftType.StructType} uses foreach loop to iterate the children, yields O(n) time for linked list
    Iterable<ThriftStructFieldInfo> scroogeFields = getFieldInfos(companionObject);
    for (ThriftStructFieldInfo field : scroogeFields) {
      children.add(toThriftField(field));
    }

    StructOrUnionType structOrUnionType =
        isUnion(companionObject.getClass()) ? StructOrUnionType.UNION : StructOrUnionType.STRUCT;

    return new ThriftType.StructType(children, structOrUnionType);
  }

  private Iterable<ThriftStructFieldInfo> getFieldInfos(ThriftStructCodec<?> c) {
    Class<? extends ThriftStructCodec> klass = c.getClass();
    if (isUnion(klass)){
      // Union needs special treatment since currently scrooge does not generates the fieldInfos
      // field in the parent union class
      return getFieldInfosForUnion(klass);
    } else {
      //each struct has a generated fieldInfos method to provide metadata to its fields
      try {
        Object r = klass.getMethod("fieldInfos").invoke(c);
        return JavaConversions$.MODULE$.asJavaIterable((scala.collection.Iterable<ThriftStructFieldInfo>)r);
      } catch (ReflectiveOperationException e) {
        throw new ScroogeSchemaConversionException("can not get field Info from: " + c.toString(), e);
      }
    }
  }


  private Iterable<ThriftStructFieldInfo> getFieldInfosForUnion(Class klass) {
    ArrayList<ThriftStructFieldInfo> fields = new ArrayList<ThriftStructFieldInfo>();
    for(Field f: klass.getDeclaredFields()){
       if (f.getType().equals(Manifest.class)) {
         Class unionClass = (Class)((ParameterizedType)f.getGenericType()).getActualTypeArguments()[0];
         Class companionUnionClass = getCompanionClass(unionClass);
         try {
           Object companionUnionObj = companionUnionClass.getField("MODULE$").get(null);
           ThriftStructFieldInfo info = (ThriftStructFieldInfo)companionUnionClass.getMethod("fieldInfo").invoke(companionUnionObj);
           fields.add(info);
         } catch (ReflectiveOperationException e) {
           throw new ScroogeSchemaConversionException("can not find fiedInfo for " + unionClass, e);
         }
      }
    }
    return fields;
  }



  /**
   * Convert a field in scrooge to ThriftField in parquet
   * @param scroogeField
   * @return
   * @throws Exception
   */
  public ThriftField toThriftField(ThriftStructFieldInfo scroogeField) {
    Requirement requirement = getRequirementType(scroogeField);
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

  private ThriftType convertSetTypeField(ThriftStructFieldInfo f, Requirement requirement) {
    return convertSetTypeField(f.tfield().name, f.valueManifest().get(), requirement);
  }

  private ThriftType convertSetTypeField(String fieldName, Manifest<?> valueManifest, Requirement requirement) {
    ThriftType elementType = convertClassToThriftType(fieldName + "_set_elem", requirement, valueManifest);
    //Set only has one sub-field as element field, therefore using hard-coded 1 as fieldId,
    //it's the same as the solution used in ElephantBird
    ThriftField elementField = generateFieldWithoutId(fieldName, requirement, elementType);
    return new ThriftType.SetType(elementField);
  }

  private ThriftType convertListTypeField(ThriftStructFieldInfo f, Requirement requirement) {
    return convertListTypeField(f.tfield().name, f.valueManifest().get(),requirement);
  }

  private ThriftType convertListTypeField(String fieldName, Manifest<?> valueManifest, Requirement requirement) {
    ThriftType elementType = convertClassToThriftType(fieldName + "_list_elem", requirement, valueManifest);
    ThriftField elementField = generateFieldWithoutId(fieldName , requirement, elementType);
    return new ThriftType.ListType(elementField);
  }

  private ThriftType convertMapTypeField(ThriftStructFieldInfo f, Requirement requirement) {
    return convertMapTypeField(f.tfield().name,f.keyManifest().get(), f.valueManifest().get(),requirement);
  }

  private ThriftType convertMapTypeField(String fieldName, Manifest<?> keyManifest, Manifest<?> valueManifest, Requirement requirement) {

    ThriftType keyType = convertClassToThriftType(fieldName + "_map_key", requirement, keyManifest);
    ThriftField keyField = generateFieldWithoutId(fieldName + "_map_key", requirement, keyType);

    ThriftType valueType = convertClassToThriftType(fieldName + "_map_value", requirement, valueManifest);
    ThriftField valueField = generateFieldWithoutId(fieldName + "_map_value", requirement, valueType);

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
   *
   * @param name
   * @param requirement
   * @param typeManifest
   * @return converted ThriftType
   * @throws Exception
   */
  private ThriftType convertClassToThriftType(String name, Requirement requirement, Manifest<?> typeManifest) {
    Class typeClass = typeManifest.runtimeClass();
    if (typeManifest.runtimeClass() == boolean.class) {
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
    } else if (typeClass == scala.collection.Seq.class){
      Manifest<?> a = typeManifest.typeArguments().apply(0);
      return convertListTypeField(name, a, requirement);
    } else if (typeClass == scala.collection.Set.class){
      Manifest<?> setElementManifest = typeManifest.typeArguments().apply(0);
      return convertSetTypeField(name, setElementManifest, requirement);
    } else if (typeClass == scala.collection.Map.class){
      List<Manifest<?>> ms = JavaConversions.seqAsJavaList(typeManifest.typeArguments());
      Manifest keyManifest = ms.get(0);
      Manifest valueManifest = ms.get(1);
      return convertMapTypeField(name, keyManifest, valueManifest, requirement);
    } else if (com.twitter.scrooge.ThriftEnum.class.isAssignableFrom(typeClass)){
      return convertEnumTypeField(typeClass, name);
    } else {
      return convertStructFromClass(typeClass);
    }
  }

  private ThriftType convertStructTypeField(ThriftStructFieldInfo f) {
    return convertStructFromClass(f.manifest().runtimeClass());
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
    return JavaConversions.seqAsJavaList((Seq)result);
  }

  public ThriftType convertEnumTypeField(ThriftStructFieldInfo f) {
    return convertEnumTypeField(f.manifest().runtimeClass(), f.tfield().name);
  }

  private ThriftType convertEnumTypeField(Class enumClass, String fieldName){
    List<ThriftType.EnumValue> enumValues = new ArrayList<ThriftType.EnumValue>();
    String enumName = enumClass.getName();
    try {
      List enumCollection = getEnumList(enumName);
      for (Object enumObj : enumCollection) {
        ScroogeEnumDesc enumDesc = ScroogeEnumDesc.fromEnum(enumObj);
        enumValues.add(new ThriftType.EnumValue(enumDesc.id, enumDesc.originalName));
      }
      return new ThriftType.EnumType(enumValues);
    } catch (ReflectiveOperationException e) {
      throw new ScroogeSchemaConversionException("Can not convert enum field " + fieldName, e);
    } catch (RuntimeException e) {
      throw new ScroogeSchemaConversionException("Can not convert enum field " + fieldName, e);
    }

  }

  //In scrooge generated class, if a class is a union, then it must have a field called "Union"
  private boolean isUnion(Class klass){
    for(Field f: klass.getDeclaredFields()) {
      if (f.getName().equals("Union"))
        return true;
    }
    return false;
  }


  private Requirement getRequirementType(ThriftStructFieldInfo f) {
    if (f.isOptional() && !f.isRequired()) {
      return OPTIONAL;
    } else if (f.isRequired() && !f.isOptional()) {
      return REQUIRED;
    } else if (!f.isOptional() && !f.isRequired()) {
      return DEFAULT;
    } else {
      throw new ScroogeSchemaConversionException("can not determine requirement type for : " + f.toString()
              + ", isOptional=" + f.isOptional() + ", isRequired=" + f.isRequired());
    }
  }

  private static class ScroogeEnumDesc {
    private int id;
    private String originalName;

    public static ScroogeEnumDesc fromEnum(Object rawScroogeEnum) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Class enumClass = rawScroogeEnum.getClass();
      Method valueMethod = enumClass.getMethod("value", new Class[]{});
      Method originalNameMethod = enumClass.getMethod("originalName", new Class[]{});
      ScroogeEnumDesc result = new ScroogeEnumDesc();
      result.id = (Integer)valueMethod.invoke(rawScroogeEnum, null);
      result.originalName = (String)originalNameMethod.invoke(rawScroogeEnum, null);
      return result;
    }
  }
}
