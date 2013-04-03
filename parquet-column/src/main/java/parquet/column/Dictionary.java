package parquet.column;

import parquet.io.api.Binary;

/**
 * a dictionary to decode dictionary based encodings
 *
 * @author Julien Le Dem
 *
 */
abstract public class Dictionary {

  private final Encoding encoding;

  public Dictionary(Encoding encoding) {
    this.encoding = encoding;
  }

  public Encoding getEncoding() {
    return encoding;
  }

  abstract public Binary decode(int id);

}
