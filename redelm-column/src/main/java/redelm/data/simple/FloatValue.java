package redelm.data.simple;

import redelm.io.RecordConsumer;

public class FloatValue extends Primitive {

  private final float value;

  public FloatValue(float value) {
    this.value = value;
  }

  @Override
  public float getFloat() {
    return value;
  }

  @Override
  public void writeValue(RecordConsumer recordConsumer) {
    recordConsumer.addFloat(value);
  }

}
