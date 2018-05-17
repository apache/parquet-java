package parquet.data.materializer;

import parquet.data.Group;
import parquet.io.ConverterConsumer;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;

public class GroupRecordConsumer extends RecordConsumer {

  private ConverterConsumer delegate;
  private GroupMaterializer materializer;

  public GroupRecordConsumer(MessageType schema) {
    materializer = new GroupMaterializer(schema);
    delegate = new ConverterConsumer(materializer.getRootConverter(), schema);
  }

  public void startMessage() {
    delegate.startMessage();
  }

  public void endMessage() {
    delegate.endMessage();
  }

  public void startField(String field, int index) {
    delegate.startField(field, index);
  }

  public void endField(String field, int index) {
    delegate.endField(field, index);
  }

  public void startGroup() {
    delegate.startGroup();
  }

  public void endGroup() {
    delegate.endGroup();
  }

  public void addInteger(int value) {
    delegate.addInteger(value);
  }

  public void addLong(long value) {
    delegate.addLong(value);
  }

  public void addBoolean(boolean value) {
    delegate.addBoolean(value);
  }

  public void addBinary(Binary value) {
    delegate.addBinary(value);
  }

  public void addFloat(float value) {
    delegate.addFloat(value);
  }

  public void addDouble(double value) {
    delegate.addDouble(value);
  }

  public Group getCurrentGroup() {
    return materializer.getCurrentRecord();
  }
}
