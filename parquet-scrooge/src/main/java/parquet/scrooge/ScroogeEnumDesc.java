package parquet.scrooge;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
* Created with IntelliJ IDEA.
* User: tdeng
* Date: 9/26/13
* Time: 4:52 PM
* To change this template use File | Settings | File Templates.
*/
class ScroogeEnumDesc {
  private int id;
  private String name;

  int getId() {
    return id;
  }

  String getName() {
    return name;
  }

  public static ScroogeEnumDesc getEnumDesc(Object rawScroogeEnum) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Class enumClass=rawScroogeEnum.getClass();
    Method valueMethod= enumClass.getMethod("value",new Class[]{});
    Method nameMethod=enumClass.getMethod("name",new Class[]{});
    ScroogeEnumDesc result= new ScroogeEnumDesc();
    result.id=(Integer)valueMethod.invoke(rawScroogeEnum,null);
    result.name=(String)nameMethod.invoke(rawScroogeEnum,null);
    return result;
  }
}
