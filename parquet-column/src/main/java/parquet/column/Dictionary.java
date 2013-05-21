package parquet.column;

import parquet.io.api.Binary;

/**
 * a dictionary to decode dictionary based encodings
 *
 * @author Julien Le Dem
 *
 */
public abstract class Dictionary {

  private final Encoding encoding;

  public Dictionary(Encoding encoding) {
    this.encoding = encoding;
  }

  public Encoding getEncoding() {
    return encoding;
  }

  public abstract int getMaxId();

  public Binary decodeToBinary(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public int decodeToInt(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public long decodeToLong(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public float decodeToFloat(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public double decodeToDouble(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }

  public boolean decodeToBoolean(int id) {
    throw new UnsupportedOperationException(this.getClass().getName());
  }
}
