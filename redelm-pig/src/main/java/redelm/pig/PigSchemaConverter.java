package redelm.pig;

import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType;
import redelm.schema.PrimitiveType.Primitive;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;

public class PigSchemaConverter {

  public MessageType convert(Schema pigSchema) {
    return new MessageType("PigSchema", convertTypes(pigSchema));
  }

  private Type[] convertTypes(Schema pigSchema) {
    List<FieldSchema> fields = pigSchema.getFields();
    Type[] types = new Type[fields.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = convert(fields.get(i));
    }
    return types;
  }

  private Type convert(FieldSchema fieldSchema) {
    try {
      switch (fieldSchema.type) {
      case DataType.BAG:
        return convertTuple(fieldSchema.alias, fieldSchema.schema.getField(0), Repetition.REPEATED);
      case DataType.TUPLE:
        return convertTuple(fieldSchema.alias, fieldSchema, Repetition.OPTIONAL);
      case DataType.MAP:
        return convertMap(fieldSchema);
      case DataType.BOOLEAN:
        return primitive(Primitive.BOOL, fieldSchema.alias);
      case DataType.CHARARRAY:
        return primitive(Primitive.STRING, fieldSchema.alias);
      case DataType.INTEGER:
        return primitive(Primitive.INT64, fieldSchema.alias);
      case DataType.LONG:
        return primitive(Primitive.INT64, fieldSchema.alias);
      case DataType.FLOAT:
        return primitive(Primitive.FLOAT, fieldSchema.alias);
      case DataType.DOUBLE:
        // TODO: right type
        return primitive(Primitive.FLOAT, fieldSchema.alias);
      case DataType.DATETIME:
        throw new UnsupportedOperationException();
      case DataType.BYTEARRAY:
        return primitive(Primitive.BINARY, fieldSchema.alias);
      default:
        throw new RuntimeException("Unknown type "+fieldSchema.type+" "+DataType.findTypeName(fieldSchema.type));
      }
    } catch (FrontendException e) {
      throw new RuntimeException("can't convert "+fieldSchema, e);
    } catch (RuntimeException e) {
      throw new RuntimeException("can't convert "+fieldSchema, e);
    }
  }

  private Type primitive(Primitive primitive, String name) {
    return new PrimitiveType(Repetition.OPTIONAL, Primitive.STRING, name);
  }

  private Type convertMap(FieldSchema fieldSchema) throws FrontendException {
    Type[] types = new Type[2];
    types[0] = new PrimitiveType(Repetition.REQUIRED, Primitive.STRING, "key");
    types[1] = convertTuple(fieldSchema.alias, fieldSchema.schema.getField(0), Repetition.REQUIRED);
    return new GroupType(Repetition.REPEATED, fieldSchema.alias, types);
  }

  private Type convertTuple(String alias, FieldSchema field, Repetition repetition) {
    return new GroupType(repetition, alias+"::"+field.alias, convertTypes(field.schema));
  }

}
