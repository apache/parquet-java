package redelm.data.simple;


public class BinaryValue extends Primitive {

  private final byte[] binary;

  public BinaryValue(byte[] binary) {
    this.binary = binary;
  }

  @Override
  public byte[] getBinary() {
    return binary;
  }
}
