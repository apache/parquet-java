package parquet.scrooge;

import com.twitter.scrooge.ThriftStructCodec;
import com.twitter.scrooge.ThriftStructField;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftTypeID;
import scala.collection.JavaConversions;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class ScroogeSchemaConverter {

  public ThriftType.StructType convertStruct(String className) throws Exception {
    Class<?> companionClass = Class.forName(className + "$");
    ThriftStructCodec cObject = (ThriftStructCodec<?>) companionClass.getField("MODULE$").get(null);

    List<ThriftField> children=new ArrayList<ThriftField>();
    Iterable<ThriftStructField> ss = JavaConversions.asIterable(cObject.metaData().fields());
    for (ThriftStructField f : ss) {
      children.add(toThriftField(f));//TODO should I check requirement here?
    }  //StructType could it self be wrapped by StructField, so no worry
    return new ThriftType.StructType(children);
  }

  public ThriftField toThriftField(ThriftStructField f) throws Exception {
    String fieldName = f.tfield().name;
    short fieldId = f.tfield().id;
    byte thriftTypeByte = f.tfield().type;
    ThriftTypeID typeId = ThriftTypeID.fromByte(thriftTypeByte);
    System.out.println(fieldName);

    ThriftType resultType=null;
    switch (ThriftTypeID.fromByte(thriftTypeByte)) {
      case STOP:
      case VOID:
      default:
        throw new UnsupportedOperationException("can't convert type");
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
        String innerName = f.method().getReturnType().getName();
        System.out.println(">>>" + innerName);
        traverseStruct(innerName);
        System.out.println("<<<" + innerName);
        break;
      case MAP:
        Type[] gTypes = ((ParameterizedType) (f.method().getGenericReturnType())).getActualTypeArguments();
        Type keyType = gTypes[0];
        Type valueType = gTypes[1];
//        traverseType(keyType,fieldName,fieldId);
//        traverseType(valueType,fieldName,fieldId);
        System.out.println("fuck");
//        final TStructDescriptor.Field mapKeyField = field.getMapKeyField();
//        final TStructDescriptor.Field mapValueField = field.getMapValueField();
//        resultType = new ThriftType.MapType(
//                toThriftField(mapKeyField.getName(), mapKeyField, requirement),
//                toThriftField(mapValueField.getName(), mapValueField, requirement));
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
      traverseStruct(innerName);
      System.out.println("<<<" + innerName);
    }
    return new ThriftField(fieldName,fieldId, ThriftField.Requirement.DEFAULT,resultType);
  }

  public ThriftType.StructType convert(Class scroogeClass) throws Exception {
      return traverseStruct(scroogeClass.getName());
  }
}
