package redelm.io;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.Deque;

final class ExpectationValidatingRecordConsumer extends
    RecordMaterializer<Void> {
  private final Deque<String> expectations;
  int count = 0;

  ExpectationValidatingRecordConsumer(Deque<String> expectations) {
    this.expectations = expectations;
  }

  private void validate(String got) {
    assertEquals("event #"+count, expectations.pop(), got);
    ++count;
  }

  @Override
  public void startMessage() {
    validate("startMessage()");
  }

  @Override
  public void startGroup() {
    validate("startGroup()");
  }

  @Override
  public void startField(String field, int index) {
    validate("startField("+field+", "+index+")");
  }

  @Override
  public void endMessage() {
    validate("endMessage()");
  }

  @Override
  public void endGroup() {
    validate("endGroup()");
  }

  @Override
  public void endField(String field, int index) {
    validate("endField("+field+", "+index+")");
  }

  @Override
  public void addString(String value) {
    validate("addString("+value+")");
  }

  @Override
  public void addInteger(int value) {
    validate("addInt("+value+")");
  }

  @Override
  public void addLong(long value) {
    validate("addLong("+value+")");
  }

  @Override
  public void addBoolean(boolean value) {
    validate("addBoolean("+value+")");
  }

  @Override
  public void addBinary(byte[] value) {
    validate("addBinary("+new BigInteger(value).toString(16)+")");
  }

  @Override
  public void addFloat(float value) {
    validate("addFloat("+value+")");
  }

  @Override
  public void addDouble(double value) {
    validate("addDouble("+value+")");
  }

  @Override
  public Void getCurrentRecord() {
    return null;
  }
}