package redelm.data.simple;

import redelm.io.RecordConsumer;

public class DoubleValue extends Primitive {

  private final double value;

  public DoubleValue(double value) {
    this.value = value;
  }

  @Override
  public double getDouble() {
    return value;
  }

  @Override
  public void writeValue(RecordConsumer recordConsumer) {
    recordConsumer.addDouble(value);
  }

}
