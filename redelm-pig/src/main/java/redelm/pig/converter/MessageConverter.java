package redelm.pig.converter;

import redelm.io.RecordMaterializer;
import redelm.schema.MessageType;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class MessageConverter extends TupleConverter {

  private static final class TupleRecordConsumer extends RecordMaterializer<Tuple> {
    private Converter currentConverter;
    private Tuple currentTuple;

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
      this.currentTuple = (Tuple)currentConverter.get();
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
      currentConverter.set(value);
    }

    @Override
    public void addLong(long value) {
      currentConverter.set(value);
    }

    @Override
    public void addInteger(int value) {
      currentConverter.set(value);
    }

    @Override
    public void addFloat(float value) {
      currentConverter.set(value);
    }

    @Override
    public void addDouble(double value) {
      currentConverter.set(value);
    }

    @Override
    public void addBoolean(boolean value) {
      currentConverter.set(value);
    }

    @Override
    public void addBinary(byte[] value) {
      currentConverter.set(value);
    }

    @Override
    public Tuple getCurrentRecord() {
      return currentTuple;
    }
  }

  public MessageConverter(MessageType redelmSchema, Schema pigSchema) throws FrontendException {
    super(redelmSchema, pigSchema, null);
  }

  public RecordMaterializer<Tuple> newRecordConsumer() {
    return new TupleRecordConsumer(this);
  }
}
