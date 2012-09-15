package redelm.data.simple;


public class StringValue extends Primitive {

  private final String value;

  public StringValue(String value) {
    this.value = value;
  }

  public String get() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }

  @Override
  public String getString() {
    return get();
  }
}
