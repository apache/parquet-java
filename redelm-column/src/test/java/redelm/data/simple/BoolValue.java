package redelm.data.simple;

public class BoolValue extends Primitive {

  private final boolean bool;
  public BoolValue(boolean bool) {
    this.bool = bool;
  }

  @Override
  public String toString() {
    return String.valueOf(bool);
  }

  @Override
  public boolean getBool() {
    return bool;
  }
}
