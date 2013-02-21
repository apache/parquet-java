package parquet.example.data.simple.convert;

import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.schema.MessageType;

public class GroupRecordConverter extends SimpleGroupConverter {

  private final SimpleGroupFactory simpleGroupFactory;

  private Group current;

  public GroupRecordConverter(MessageType schema) {
    super(null, -1, schema);
    this.simpleGroupFactory = new SimpleGroupFactory(schema);
  }

  @Override
  public Group getCurrentRecord() {
    return current;
  }

  @Override
  public void start() {
    current = simpleGroupFactory.newGroup();
  }

  @Override
  public void end() {
  }

}
