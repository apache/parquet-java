package redelm.column.primitive;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class SimplePrimitiveColumnReader extends PrimitiveColumnReader {

  private DataInputStream in;

  public SimplePrimitiveColumnReader(byte[] data, int offset, int length) {
    this.in = new DataInputStream(new ByteArrayInputStream(data, offset, length));
  }

  @Override
  public float readFloat() {
    try {
      return in.readFloat();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public byte[] readBytes() {
    try {
      byte[] value = new byte[in.readInt()];
      in.readFully(value);
      return value;
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public boolean readBoolean() {
    try {
      return in.readBoolean();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public String readString() {
    try {
      return in.readUTF();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public double readDouble() {
    try {
      return in.readDouble();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public int readInt() {
    try {
      return in.readInt();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

  @Override
  public int readByte() {
    try {
      return in.read();
    } catch (IOException e) {
      throw new RuntimeException("never happens", e);
    }
  }

}
