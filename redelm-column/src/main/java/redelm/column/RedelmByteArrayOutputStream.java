package redelm.column;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RedelmByteArrayOutputStream extends ByteArrayOutputStream {

  public RedelmByteArrayOutputStream(int initialSize) {
    super(initialSize);
  }

  public void writeTo(BytesOutput out) throws IOException {
    out.write(buf, 0, count);
  }

}
