package parquet.column.page;

import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.column.Encoding;

/**
 * Data for a dictionary page
 *
 * @author Julien Le Dem
 *
 */
public class DictionaryPage {

  private final BytesInput bytes;
  private final int uncompressedSize;
  private final int dictionarySize;
  private final Encoding encoding;

  /**
   * creates an uncompressed page
   * @param bytes the content of the page
   * @param dictionarySize the value count in the dictionary
   * @param encoding the encoding used
   */
  public DictionaryPage(BytesInput bytes, int dictionarySize, Encoding encoding) {
    this(bytes, (int)bytes.size(), dictionarySize, encoding); // TODO: fix sizes long or int
  }

  /**
   * creates a dictionary page
   * @param bytes the (possibly compressed) content of the page
   * @param uncompressedSize the size uncompressed
   * @param dictionarySize the value count in the dictionary
   * @param encoding the encoding used
   */
  public DictionaryPage(BytesInput bytes, int uncompressedSize, int dictionarySize, Encoding encoding) {
    if (bytes == null) {
      throw new NullPointerException("bytes");
    }
    if (encoding == null) {
      throw new NullPointerException("encoding");
    }
    this.bytes = bytes;
    this.uncompressedSize = uncompressedSize;
    this.dictionarySize = dictionarySize;
    this.encoding = encoding;
  }

  public BytesInput getBytes() {
    return bytes;
  }

  public int getUncompressedSize() {
    return uncompressedSize;
  }

  public int getDictionarySize() {
    return dictionarySize;
  }

  public Encoding getEncoding() {
    return encoding;
  }

  public DictionaryPage copy() throws IOException {
    return new DictionaryPage(BytesInput.copy(bytes), uncompressedSize, dictionarySize, encoding);
  }


  @Override
  public String toString() {
    return "Page [bytes.size=" + bytes.size() + ", entryCount=" + dictionarySize + ", uncompressedSize=" + uncompressedSize + ", encoding=" + encoding + "]";
  }


}
