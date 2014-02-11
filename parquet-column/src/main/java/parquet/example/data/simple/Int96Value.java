package parquet.example.data.simple;

import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;

public class Int96Value extends Primitive {

  private final Binary value;

  public Int96Value(Binary value) {
    this.value = value;
  }

  @Override
  public Binary getInt96() {
    return value;
  }

  @Override
  public void writeValue(RecordConsumer recordConsumer) {
    recordConsumer.addBinary(value);
  }

  @Override
  public String toString() {
    return "Int96Value{" + String.valueOf(value) + "}";
  }
}
