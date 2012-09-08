package redelm.io;

abstract public class RecordConsumer {

  abstract public void startMessage();
  abstract public void endMessage();

  abstract public void startField(String field, int index);
  abstract public void endField(String field, int index);

  abstract public void startGroup();
  abstract public void endGroup();

  abstract public void addInt(int value);
  abstract public void addString(String value);
  abstract public void addBoolean(boolean value);
  abstract public void addBinary(byte[] value);

}
