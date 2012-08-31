package redelm.data.simple;

import redelm.data.Group;
import redelm.data.GroupFactory;
import redelm.schema.MessageType;

public class SimpleGroupFactory extends GroupFactory {

  private final MessageType schema;

  public SimpleGroupFactory(MessageType schema) {
    this.schema = schema;
  }

  @Override
  public Group newGroup() {
    return new SimpleGroup(schema);
  }

}
