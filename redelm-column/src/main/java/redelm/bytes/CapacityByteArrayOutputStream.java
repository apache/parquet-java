package redelm.bytes;

import java.io.ByteArrayOutputStream;

/**
 * expose the memory used by a ByteArrayOutputStream
 *
 * @author Julien Le Dem
 *
 */
public class CapacityByteArrayOutputStream extends ByteArrayOutputStream {

  public CapacityByteArrayOutputStream(int initialSize) {
    super(initialSize);
  }

  public int getCapacity() {
    return buf.length;
  }

}