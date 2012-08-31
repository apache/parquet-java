package redelm.thrift;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import trajan.schema.Type;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TType;

import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;

public class ThriftRandomGenerator<T extends TBase<?,?>> {

  private TStructDescriptor descriptor;

  public ThriftRandomGenerator(Class<T> c) {
    descriptor = TStructDescriptor.getInstance(c);
  }

  public T newInstance() {
    try {
      return (T)randomStruct(descriptor);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private TBase<?, ?> randomStruct(TStructDescriptor tStructDescriptor)
      throws TException {
    TBase<?, TFieldIdEnum> newStruct = (TBase<?, TFieldIdEnum>)tStructDescriptor.newThriftObject();
    List<Field> fields = tStructDescriptor.getFields();
    Type[] result = new Type[fields.size()];
    for (int i = 0; i < result.length; i++) {
      Field field = fields.get(i);
      TFieldIdEnum fieldIdEnum = (TFieldIdEnum)field.getFieldIdEnum();
      newStruct.setFieldValue(fieldIdEnum, randomValue(field));
    }
    return newStruct;
  }

  private Object randomValue(Field field) throws TException {
    Object value;
    if (field.isList()) {
      List<Object> list = new ArrayList<Object>();
      for (int j = 0; j < 5; j++) {
        list.add(randomValue(field.getListElemField()));
      }
      value = list;
    } else if (field.isSet()) {
      Set<Object> set = new HashSet<Object>();
      for (int j = 0; j < 5; j++) {
        set.add(randomValue(field.getSetElemField()));
      }
      value = set;
    } else if (field.isBuffer()) {
      value = "";
    } else if (field.isEnum()) {
      value = null;
    } else if (field.isStruct()) {
      value = randomStruct(field.gettStructDescriptor());
    } else {
      value = randomPrimitive(field);
    }
    return value;
  }

  private static Object randomPrimitive(Field field) {

    switch (field.getType()) {
    case TType.I64:
      return Math.round(Math.random()*Long.MAX_VALUE);
    case TType.STRING:
      return String.valueOf(Math.random());
    case TType.BOOL:
      return Math.random()>0.5;
    case TType.I32:
      return (int)Math.round(Math.random()*Integer.MAX_VALUE);
    case TType.MAP:
      return new HashMap<Object, Object>();
    case TType.STOP:
    case TType.VOID:
    case TType.BYTE:
    case TType.DOUBLE:
    case TType.I16:
    case TType.STRUCT:
    case TType.SET:
    case TType.LIST:
    case TType.ENUM:
      default:
        throw new RuntimeException("unsupported type "+field.getType());
    }
  }


}
