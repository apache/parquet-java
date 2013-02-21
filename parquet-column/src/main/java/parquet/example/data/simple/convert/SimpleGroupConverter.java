package parquet.example.data.simple.convert;

import parquet.example.data.Group;
import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.io.convert.RecordConverter;
import parquet.schema.GroupType;
import parquet.schema.Type;

class SimpleGroupConverter extends RecordConverter<Group> {
  private final SimpleGroupConverter parent;
  private final int index;
  private Group current;
  private GroupConverter[] groupConverters;
  private PrimitiveConverter[] primitiveConverters;

  SimpleGroupConverter(SimpleGroupConverter parent, int index, GroupType schema) {
    this.parent = parent;
    this.index = index;

    groupConverters = new GroupConverter[schema.getFieldCount()];
    primitiveConverters = new PrimitiveConverter[schema.getFieldCount()];

    for (int i = 0; i < groupConverters.length; i++) {
      final Type type = schema.getType(i);
      if (type.isPrimitive()) {
        primitiveConverters[i] = new SimplePrimitiveConverter(this, i);
      } else {
        groupConverters[i] = new SimpleGroupConverter(this, i, type.asGroupType());
      }

    }
  }

  @Override
  public void start() {
    current = parent.getCurrentRecord().addGroup(index);
  }

  @Override
  public PrimitiveConverter getPrimitiveConverter(int fieldIndex) {
    return primitiveConverters[fieldIndex];
  }

  @Override
  public GroupConverter getGroupConverter(int fieldIndex) {
    return groupConverters[fieldIndex];
  }

  @Override
  public void end() {
  }

  @Override
  public Group getCurrentRecord() {
    return current;
  }
}