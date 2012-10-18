package redelm.column;

import java.io.IOException;

abstract public class BytesOutput {

  public abstract void write(byte[] bytes, int index, int length) throws IOException;

}
