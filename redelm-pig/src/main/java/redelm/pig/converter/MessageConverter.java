package redelm.pig.converter;

import redelm.io.RecordConsumer;

public class MessageConverter extends TupleConverter {

  private static final class TupleRecordConsumer extends RecordConsumer {
    private Converter currentConverter;

    public TupleRecordConsumer(MessageConverter messageConverter) {
      this.currentConverter = messageConverter;
    }

    @Override
    public void startMessage() {
      currentConverter.start();
    }

    @Override
    public void startGroup() {
      this.currentConverter = currentConverter.startGroup();
      currentConverter.start();
    }

    @Override
    public void startField(String field, int index) {
      currentConverter.startField(field, index);
    }

    @Override
    public void endMessage() {
      currentConverter.end();
    }

    @Override
    public void endGroup() {
      currentConverter = currentConverter.end();
      this.currentConverter.endGroup();
    }

    @Override
    public void endField(String field, int index) {
      currentConverter.endField(field, index);
    }

    @Override
    public void addString(String value) {
      // TODO Auto-generated method stub

    }

    @Override
    public void addLong(long value) {
      // TODO Auto-generated method stub

    }

    @Override
    public void addInteger(int value) {
      // TODO Auto-generated method stub

    }

    @Override
    public void addFloat(float value) {
      // TODO Auto-generated method stub

    }

    @Override
    public void addDouble(double value) {
      // TODO Auto-generated method stub

    }

    @Override
    public void addBoolean(boolean value) {
      // TODO Auto-generated method stub

    }

    @Override
    public void addBinary(byte[] value) {
      // TODO Auto-generated method stub

    }
  }

  public RecordConsumer newRecordConsumer() {
    return new TupleRecordConsumer(this);

  }
}
