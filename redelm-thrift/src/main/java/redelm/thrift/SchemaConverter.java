package redelm.thrift;

import static redelm.schema.PrimitiveType.Primitive.INT64;
import static redelm.schema.PrimitiveType.Primitive.STRING;
import static redelm.schema.Type.Repetition.OPTIONAL;
import static redelm.schema.Type.Repetition.REPEATED;
import static redelm.schema.Type.Repetition.REQUIRED;

import java.util.List;

import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType;
import redelm.schema.Type;
import redelm.schema.PrimitiveType.Primitive;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TType;

import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;

public class SchemaConverter {

  public MessageType toMessage(Class<? extends TBase<?,?>> clazz) {
    TStructDescriptor struct = TStructDescriptor.getInstance(clazz);
    return new MessageType(
        "Document",
        toSchema(struct)
    );
  }

  private Type[] toSchema(TStructDescriptor struct) {
    List<Field> fields = struct.getFields();
    Type[] result = new Type[fields.size()];
    for (int i = 0; i < result.length; i++) {
      Field field = fields.get(i);
      FieldMetaData tField = field.getFieldMetaData();
      Type.Repetition rep;
      switch (tField.requirementType) {
      case TFieldRequirementType.REQUIRED:
        rep = REQUIRED;
        break;
      case TFieldRequirementType.OPTIONAL:
        rep = OPTIONAL;
        break;
      case TFieldRequirementType.DEFAULT:
        rep = REQUIRED; // ???
        break;
      default:
        throw new IllegalArgumentException("unknown requirement type: " + tField.requirementType);
      }
      if (field.isList()) {
        result[i] = toSchema(field.getName(), field.getListElemField(), REPEATED);
      } else if (field.isSet()) {
        result[i] = toSchema(field.getName(), field.getSetElemField(), REPEATED);
      } else {
        result[i] = toSchema(field.getName(), field, rep);
      }
    }
    return result;
  }

  private Type toSchema(String name, Field field, Type.Repetition rep) {
    if (field.isList()) {
      throw new RuntimeException();
    } else if (field.isSet()) {
      throw new RuntimeException();
    } else if (field.isStruct()) {
      return new GroupType(rep, field.getName(), toSchema(field.gettStructDescriptor()));
    } else if (field.isBuffer()) {
      return new PrimitiveType(rep, STRING, field.getName());
    } else if (field.isEnum()) {
      return new PrimitiveType(rep, INT64, field.getName());
    } else {
      PrimitiveType.Primitive p = getPrimitive(field);
      return new PrimitiveType(rep, p, field.getName());
    }
  }

  private PrimitiveType.Primitive getPrimitive(Field field) {
    PrimitiveType.Primitive p;
    switch (field.getType()) {
    case TType.I64:
      p=INT64;
      break;
    case TType.STRING:
      p = STRING;
      break;
    case TType.BOOL:
      p = Primitive.BOOL;
      break;
    case TType.I32:
      p=INT64; // TODO
      break;
    case TType.MAP:
      p=Primitive.BINARY; // TODO
      break;
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
    return p;
  }

}
