package redelm.data.simple;


public class IntValue extends Primitive {

  private final int value;

  public IntValue(int value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }

  @Override
  public int getInt() {
    return value;
  }
}
