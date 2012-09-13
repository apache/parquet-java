package redelm.data;

import redelm.data.Group;
import redelm.io.RecordConsumer;
import redelm.schema.GroupType;
import redelm.schema.Type;

public class GroupWriter {

  private final RecordConsumer recordConsumer;
  private final GroupType schema;

  public GroupWriter(RecordConsumer recordConsumer, GroupType schema) {
    this.recordConsumer = recordConsumer;
    this.schema = schema;
  }

  public void write(Group group) {
    recordConsumer.startMessage();
    writeGroup(group, schema);
    recordConsumer.endMessage();
  }

  private void writeGroup(Group group, GroupType type) {
    int fieldCount = type.getFieldCount();
    for (int field = 0; field < fieldCount; ++field) {
      int valueCount = group.getFieldRepetitionCount(field);
      if (valueCount > 0) {
        Type fieldType = type.getType(field);
        String fieldName = fieldType.getName();
        recordConsumer.startField(fieldName, field);
        for (int index = 0; index < valueCount; ++index) {
          if (fieldType.isPrimitive()) {
            fieldType.asPrimitiveType().getPrimitive().addValueToRecordConsumer(recordConsumer, group, field, index);
          } else {
            recordConsumer.startGroup();
            writeGroup(group.getGroup(field, index), fieldType.asGroupType());
            recordConsumer.endGroup();
          }
        }
        recordConsumer.endField(fieldName, field);
      }
    }
  }

}
