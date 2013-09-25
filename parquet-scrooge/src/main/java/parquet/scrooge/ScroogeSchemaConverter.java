package parquet.scrooge;

import com.twitter.scrooge.ThriftStructCodec;
import com.twitter.scrooge.ThriftStructField;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftTypeID;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.reflect.Manifest;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class ScroogeSchemaConverter {

  public ThriftType.StructType convertStructFromClassName(String className) throws Exception {
    Class<?> companionClass = Class.forName(className + "$");
    ThriftStructCodec cObject = (ThriftStructCodec<?>) companionClass.getField("MODULE$").get(null);

    List<ThriftField> children = new ArrayList<ThriftField>();
    Iterable<ThriftStructField> ss = JavaConversions.asIterable(cObject.metaData().fields());
    for (ThriftStructField f : ss) {
      children.add(toThriftField(f));//TODO should I check requirement here?
    }  //StructType could it self be wrapped by StructField, so no worry
    return new ThriftType.StructType(children);
  }

  public ThriftField toThriftField(ThriftStructField f) throws Exception {
    //TODO: if the return type is Option then set the requirement to be Optional
    ThriftField.Requirement requirement = ThriftField.Requirement.REQUIRED;
    if (isOptional(f)) {
        requirement=ThriftField.Requirement.OPTIONAL;
    }
    //TODO: default to optional or required????

    String fieldName = f.tfield().name;
    short fieldId = f.tfield().id;
    byte thriftTypeByte = f.tfield().type;
    ThriftTypeID typeId = ThriftTypeID.fromByte(thriftTypeByte);
    System.out.println(fieldName);

    ThriftType resultType = null;
    switch (typeId) {
      case STOP:
      case VOID:
      default:
        throw new UnsupportedOperationException("can't convert type");
        // Primitive type can be inspected from type of TField, it should be accurate
      case BOOL:
        resultType = new ThriftType.BoolType();
        break;
      case BYTE:
        resultType = new ThriftType.ByteType();
        break;
      case DOUBLE:
        resultType = new ThriftType.DoubleType();
        break;
      case I16:
        resultType = new ThriftType.I16Type();
        break;
      case I32:
        resultType = new ThriftType.I32Type();
        break;
      case I64:
        resultType = new ThriftType.I64Type();
        break;
      case STRING:
        resultType = new ThriftType.StringType();
        break;
      case STRUCT:
        resultType= convertStructTypeField(f);
        break;
      case MAP:
        resultType=convertMapTypeField(f);

        break;
      case SET:
//        final TStructDescriptor.Field setElemField = field.getSetElemField();
//        resultType = new ThriftType.SetType(toThriftField(name, setElemField, requirement));
        break;
      case LIST:
//        final TStructDescriptor.Field listElemField = field.getListElemField();
//        resultType = new ThriftType.ListType(toThriftField(name, listElemField, requirement));
        break;
      case ENUM:
//        Collection<TEnum> enumValues = field.getEnumValues();
//        List<ThriftType.EnumValue> values = new ArrayList<ThriftType.EnumValue>();
//        for (TEnum tEnum : enumValues) {
//          values.add(new ThriftType.EnumValue(tEnum.getValue(), tEnum.toString()));
//        }
//        resultType = new ThriftType.EnumType(values);
        break;
    }


    if (ThriftTypeID.fromByte(f.tfield().type) == ThriftTypeID.STRUCT) {
      String innerName = f.method().getReturnType().getName();
      System.out.println(">>>" + innerName);
//      traverseStruct(innerName);
      System.out.println("<<<" + innerName);
    }
    return new ThriftField(fieldName, fieldId, requirement, resultType);
  }

  private List<Class> getTypeArguments(ThriftStructField f){
//    ((scala.reflect.Manifest)(((scala.reflect.Manifest)f.manifest().get()).typeArguments().first())).erasure()==short.class
    Iterator<Manifest> it = ((Manifest) f.manifest().get()).typeArguments().iterator();
    List<Class> types=new ArrayList<Class>();
    while(it.hasNext()){
      types.add(it.next().erasure());
    }
    return types;
  }

  private ThriftType convertMapTypeField(ThriftStructField f) {
    Type mapType=null;
    if (isOptional(f)){
      mapType=extractClassFromOption(f.method().getGenericReturnType());
    }else{
      mapType=f.method().getGenericReturnType();
    }
    List<Class> typeArguments=getTypeArguments(f);//TODO test optional fields
    Class keyClass = typeArguments.get(0);
    //TODO requirement should be the requirement of the map
    ThriftType keyType=convertBasedOnClass(keyClass);
    Class valueClass = typeArguments.get(1);
    //TODO: what is the id of a key???default to 1, this is the behavior in elephant bird
    //TODO:requirementType??
    ThriftField keyField=new ThriftField(f.name()+"_map_key", (short) 1, ThriftField.Requirement.REQUIRED,keyType);
    ThriftType valueType=convertBasedOnClass(valueClass);
    ThriftField valueField=new ThriftField(f.name()+"_map_value", (short) 1, ThriftField.Requirement.REQUIRED,valueType);
    return new ThriftType.MapType(keyField,valueField);

    //TODO notice the key and value field could be String..boolean, or complexType
//        traverseType(keyType,fieldName,fieldId);
//        traverseType(valueType,fieldName,fieldId);
//        final TStructDescriptor.Field mapKeyField = field.getMapKeyField();
//        final TStructDescriptor.Field mapValueField = field.getMapValueField();
//        resultType = new ThriftType.MapType(
//                toThriftField(mapKeyField.getName(), mapKeyField, requirement),
//                toThriftField(mapValueField.getName(), mapValueField, requirement));
  }

  private ThriftType convertBasedOnClass(Class keyClass) {
//    if (keyClass==Boolean.class){
//      return new ThriftType.BoolType();
//    }else if (keyClass==Byte.class){
//      return new ThriftType.ByteType();
//    }else if (keyClass==Double.class){
//      return new ThriftType.DoubleType();
//    }else if (keyClass==Short.class){
//      return new ThriftType.I16Type();
//    }
     //This will be used by generic types, like map, list, set
    if (keyClass==boolean.class){
      return new ThriftType.BoolType();
    }else if (keyClass==byte.class){
      return new ThriftType.ByteType();
    }else if (keyClass==double.class){
      return new ThriftType.DoubleType();
    }else if (keyClass==short.class){
      return new ThriftType.I16Type();
    }else if (keyClass==int.class){
      return new ThriftType.I32Type();
    }else if (keyClass==long.class){
      return new ThriftType.I64Type();
    }else if (keyClass==String.class){
      return new ThriftType.StringType();
    }
    return null;
  }

  private ThriftType convertStructTypeField(ThriftStructField f) throws Exception {
    Type structClassType=f.method().getReturnType();
    if(isOptional(f)){
      structClassType=extractClassFromOption(f.method().getGenericReturnType());
    }
    return convertStructFromClassName(((Class) structClassType).getName());
  }

  private Type extractClassFromOption(Type genericReturnType) {
    return ((ParameterizedType)genericReturnType).getActualTypeArguments()[0];
  }

  private boolean isOptional(ThriftStructField f) {
    return f.method().getReturnType() == scala.Option.class;
  }

  public ThriftType.StructType convert(Class scroogeClass) throws Exception {
    return convertStructFromClassName(scroogeClass.getName());
  }
}
