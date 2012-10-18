package redelm.column.primitive;

import java.io.IOException;
import java.io.OutputStream;

import redelm.column.BytesOutput;

public abstract class PrimitiveColumnWriter {

  public abstract int getMemSize();

  public abstract void writeData(BytesOutput out) throws IOException;

  public abstract void reset();

  public void writeByte(int value) {
    throw new UnsupportedOperationException();
  }

  public void writeBoolean(boolean v) {
    throw new UnsupportedOperationException();
  }

  public void writeBytes(byte[] v) {
    throw new UnsupportedOperationException();
  }

  public void writeInt(int v) {
    throw new UnsupportedOperationException();
  }

  public void writeLong(long v) {
    throw new UnsupportedOperationException();
  }

  public void writeDouble(double v) {
    throw new UnsupportedOperationException();
  }

  public void writeString(String str) {
    throw new UnsupportedOperationException();
  }

  public void writeFloat(float v) {
    throw new UnsupportedOperationException();
  }

}
