package redelm.thrift;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import redelm.data.Group;
import redelm.data.GroupFactory;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;

public class ThriftConverter<T extends TBase<?,TFieldIdEnum>> {
  private TStructDescriptor descriptor;
  private MessageType schema;
  private final GroupFactory groupFactory;

  public ThriftConverter(Class<T> c, MessageType schema, GroupFactory groupFactory) {
    this.groupFactory = groupFactory;
    this.descriptor = TStructDescriptor.getInstance(c);
  }

  public Group convert(T instance) {
    Group newGroup = groupFactory.newGroup();
    convertGroup(descriptor.getFields(), schema, instance, newGroup);
    return newGroup;
  }

  private void convertGroup(List<Field> thriftFields, GroupType groupType, TBase<?,TFieldIdEnum> instance, Group to) {
    List<Type> fields = schema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      Field thriftField = thriftFields.get(i);
      Type field = fields.get(i);
      convertField(instance, thriftField, to, field);
    }
  }

  private void convertField(TBase<?,TFieldIdEnum> from, Field fromField, Group to, Type toField) {
    if (toField.getRepetition() == Repetition.REQUIRED) {
      addValue(from, fromField, to, toField);
    } else if (toField.getRepetition() == Repetition.REPEATED) {
      Object fieldValue = from.getFieldValue(fromField.getFieldIdEnum());
      if (fromField.isList()) {
        List<Object> list = (List)fieldValue;
afl;g'kkdafl;gk;l
s
      } else if (fromField.isSet()) {
        Set<Object> set = (Set) fieldValue;
        lsd
        ;akgl;kafd
      } else {
        throw new RuntimeException("Unknown REPEATED field "+toField);
      }
    } else if (toField.getRepetition() == Repetition.OPTIONAL) {

    } else {
      throw new RuntimeException("Unknown repetition "+toField.getRepetition());
    }

  }

  private void addValue(TBase<?,TFieldIdEnum> from, Field fromField, Group to, Type toField) {
    if (toField.isPrimitive()) {

    } else {
      convertGroup(
          fromField.gettStructDescriptor().getFields(),
          toField.asGroupType(),
          (TBase<?,TFieldIdEnum>)from.getFieldValue(fromField.getFieldIdEnum()),
          to.addGroup(toField.getName()));
    }
  }

}
