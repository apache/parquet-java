package parquet.data.materializer;

import static parquet.schema.Type.Repetition.REPEATED;
import parquet.data.Group;
import parquet.data.GroupWriter;
import parquet.io.ParquetEncodingException;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

public class GroupWriterImpl extends GroupWriter {

  private final RecordConsumer recordConsumer;
  private final MessageType schema;

  // TODO: make this compiled too
  public static GroupWriter newGroupWriter(RecordConsumer recordConsumer, MessageType schema) {
    return new GroupWriterImpl(recordConsumer, schema);
  }

  private GroupWriterImpl(RecordConsumer recordConsumer, MessageType schema) {
    this.recordConsumer = recordConsumer;
    this.schema = schema;
  }

  @Override
  public void write(Group group) {
    recordConsumer.startMessage();
    writeGroup(group, schema);
    recordConsumer.endMessage();
  }

  // TODO: this somewhat complex method is a first step before compiling the logic
  private void writeGroup(Group group, GroupType type) {
    int fieldCount = type.getFieldCount();
    for (int field = 0; field < fieldCount; ++field) {
      Type fieldType = type.getType(field);
      String fieldName = fieldType.getName();
      try {
        if (fieldType.isRepetition(REPEATED)) {
          int valueCount = group.getRepetitionCount(field);
          if (valueCount > 0) {
            recordConsumer.startField(fieldName, field);
            for (int index = 0; index < valueCount; ++index) {
              if (fieldType.isPrimitive()) {
                switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
                case INT64:
                  recordConsumer.addLong(group.getRepeatedLongAt(field, index));
                  break;
                case INT32:
                  recordConsumer.addInteger(group.getRepeatedIntAt(field, index));
                  break;
                case FLOAT:
                  recordConsumer.addFloat(group.getRepeatedFloatAt(field, index));
                  break;
                case DOUBLE:
                  recordConsumer.addDouble(group.getRepeatedDoubleAt(field, index));
                  break;
                case BOOLEAN:
                  recordConsumer.addBoolean(group.getRepeatedBooleanAt(field, index));
                  break;
                case BINARY:
                case INT96:
                case FIXED_LEN_BYTE_ARRAY:
                  recordConsumer.addBinary(group.getRepeatedBinaryAt(field, index));
                  break;
                default: throw new ParquetEncodingException("Unknown type: " + fieldType.asPrimitiveType().getPrimitiveTypeName());
                }
              } else {
                recordConsumer.startGroup();
                writeGroup(group.getRepeatedGroupAt(field, index), fieldType.asGroupType());
                recordConsumer.endGroup();
              }
            }
            recordConsumer.endField(fieldName, field);
          }
        } else {
          if (group.isDefined(field)) {
            recordConsumer.startField(fieldName, field);
            if (fieldType.isPrimitive()) {
              switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
              case INT64:
                recordConsumer.addLong(group.getLong(field));
                break;
              case INT32:
                recordConsumer.addInteger(group.getInt(field));
                break;
              case FLOAT:
                recordConsumer.addFloat(group.getFloat(field));
                break;
              case DOUBLE:
                recordConsumer.addDouble(group.getDouble(field));
                break;
              case BOOLEAN:
                recordConsumer.addBoolean(group.getBoolean(field));
                break;
              case BINARY:
              case INT96:
              case FIXED_LEN_BYTE_ARRAY:
                recordConsumer.addBinary(group.getBinary(field));
                break;
              default: throw new ParquetEncodingException("Unknown type: " + fieldType.asPrimitiveType().getPrimitiveTypeName());
              }
            } else {
              recordConsumer.startGroup();
              writeGroup(group.getGroup(field), fieldType.asGroupType());
              recordConsumer.endGroup();
            }
            recordConsumer.endField(fieldName, field);
          }
        }
      } catch (RuntimeException e) {
        throw new ParquetEncodingException("field: " + field + " type: " + fieldType, e);
      }
    }
  }
}
