package parquet.io;

import static org.junit.Assert.assertEquals;

import java.util.Deque;

import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.io.convert.RecordConverter;

public class ExpectationValidatingConverter extends ExpectationValidatingGroupConverter {

  public ExpectationValidatingConverter(Deque<String> expectations) {
    super(null, new Validator(expectations));
  }

  @Override
  public Void getCurrentRecord() {
    return null;
  }

  @Override
  public void start() {
    validate("startMessage()");
  }

  @Override
  public void end() {
    validate("endMessage()");
  }

}

class Validator {
  private final Deque<String> expectations;
  int count = 0;

  public Validator(Deque<String> expectations) {
    this.expectations = expectations;
  }

  public void validate(String got) {
    assertEquals("event #"+count, expectations.pop(), got);
    ++count;
  }

}

class ExpectationValidatingGroupConverter extends RecordConverter<Void> {

  private final String path;
  private final Validator validator;

  public ExpectationValidatingGroupConverter(String path, Validator validator) {
    this.path = path;
    this.validator = validator;
  }

  public void validate(String s) {
    validator.validate((path == null ? "" : path + ".") + s);
  }

  @Override
  public Void getCurrentRecord() {
    return null;
  }

  @Override
  public GroupConverter getGroupConverter(int fieldIndex) {
    return new ExpectationValidatingGroupConverter((path == null ? "" : path + ".") + fieldIndex, validator);
  }

  @Override
  public PrimitiveConverter getPrimitiveConverter(int fieldIndex) {
    return new ExpectationValidatingPrimitiveConverter((path == null ? "" : path + ".") + fieldIndex, validator);
  }

  @Override
  public void start() {
    validate("start()");
  }

  @Override
  public void end() {
    validate("end()");
  }

}

class ExpectationValidatingPrimitiveConverter extends PrimitiveConverter {

  private final String path;
  private final Validator validator;

  public ExpectationValidatingPrimitiveConverter(String path,
      Validator validator) {
        this.path = path;
        this.validator = validator;
  }

  public void validate(String s) {
    validator.validate((path == null ? "" : path + ".") + s);
  }

  @Override
  public void addBinary(byte[] value) {
    validate("addBinary("+new String(value)+")");
  }

  @Override
  public void addBoolean(boolean value) {
    validate("addBoolean("+value+")");
  }

  @Override
  public void addDouble(double value) {
    validate("addDouble("+value+")");
  }

  @Override
  public void addFloat(float value) {
    validate("addFloat("+value+")");
  }

  @Override
  public void addInt(int value) {
    validate("addInt("+value+")");
  }

  @Override
  public void addLong(long value) {
    validate("addLong("+value+")");
  }

}