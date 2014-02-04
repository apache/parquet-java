package parquet.example.data.simple.convert;

import parquet.example.data.simple.Primitive;
import parquet.io.api.Int96;
import parquet.io.api.RecordConsumer;

public class Int96Value extends Primitive {

  private final Int96 value;

  public Int96Value(Int96 value) {
    this.value = value;
  }

  @Override
  public Int96 getInt96() {
    return value;
  }

  @Override
  public void writeValue(RecordConsumer recordConsumer) {
    recordConsumer.addInt96(value);
  }

  @Override
  public String toString() {
    return "Int96Value{" + String.valueOf(value) + "}";
  }
}
