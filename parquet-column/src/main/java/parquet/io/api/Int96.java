package parquet.io.api;

import java.nio.ByteBuffer;

public class Int96 {
  private final ByteBuffer bytes;

  public static Int96 fromByteBuffer(ByteBuffer bytes) {
    return new Int96(bytes);
  }

  public Int96(ByteBuffer bytes) {
    this.bytes = bytes;
  }

  public ByteBuffer toByteBuffer() {
    return bytes;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{" + String.valueOf(bytes) + "}";
  }
}
