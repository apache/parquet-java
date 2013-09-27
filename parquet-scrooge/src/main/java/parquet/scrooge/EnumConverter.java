package parquet.scrooge;

import com.twitter.scrooge.ThriftStructField;
import parquet.thrift.struct.ThriftType;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class EnumConverter {

  //When define an enum in scrooge, each enum value is a subclass of the enum class, the enum class could be Operation$
  private List getEnumList(String enumName) throws ClassNotFoundException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException, InvocationTargetException {
    enumName+="$";//In scala generated code the actual class is ended in $
    Class companionObjectClass= Class.forName(enumName);
    Object cObject=companionObjectClass.getField("MODULE$").get(null);

    Method listMethod = companionObjectClass.getMethod("list",new Class[]{});
    Object result=listMethod.invoke(cObject,null);
    return JavaConversions.asJavaList((Seq) result);
  }

  public ThriftType convertEnumTypeField(ThriftStructField f) {
    //TODO arrayList or linked List
    List<ThriftType.EnumValue> enumValues=new ArrayList<ThriftType.EnumValue>();
    String enumName = f.method().getReturnType().getName();
    try {
      List enumCollection = getEnumList(enumName);
      for(Object enumObj:enumCollection){
        ScroogeEnumDesc enumDesc=ScroogeEnumDesc.getEnumDesc(enumObj);
        //TODO for compatible with thrift generated enum which have Capitalized name
        enumValues.add(new ThriftType.EnumValue(enumDesc.getId(),enumDesc.getName().toUpperCase()));
      }
      return new ThriftType.EnumType(enumValues);

    } catch (Exception e) {
      return null;//TODO rethrow the right exception
//      throw new Exception("fucked",e);
    }
  }
}
