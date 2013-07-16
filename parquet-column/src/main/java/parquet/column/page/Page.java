package parquet.column.page;

import static parquet.Preconditions.checkNotNull;
import parquet.bytes.BytesInput;

/**
 * A Page in a column chunk
 *
 * @author Julien Le Dem
 *
 */
public abstract class Page {

  private final BytesInput bytes;
  private final int compressedSize;
  private final int uncompressedSize;

  Page(BytesInput bytes, int compressedSize, int uncompressedSize) {
    super();
    this.bytes = checkNotNull(bytes, "bytes");
    this.compressedSize = compressedSize;
    this.uncompressedSize = uncompressedSize;
  }

  /**
   * @return the bytes for the page
   */
  public BytesInput getBytes() {
    return bytes;
  }

  /**
   * @return the compressed size of the page when the bytes are compressed
   */
  public int getCompressedSize() {
    return compressedSize;
  }

  /**
   * @return the uncompressed size of the page when the bytes are uncompressed
   */
  public int getUncompressedSize() {
    return uncompressedSize;
  }

  /**
   * @param visitor the visitor to accept
   */
  abstract public void accept(PageVisitor visitor);

}
