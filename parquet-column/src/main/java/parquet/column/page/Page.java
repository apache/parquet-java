package parquet.column.page;

/**
 * one page in a chunk
 *
 * @author Julien Le Dem
 *
 */
abstract public class Page {

  private final int compressedSize;
  private final int uncompressedSize;

  Page(int compressedSize, int uncompressedSize) {
    super();
    this.compressedSize = compressedSize;
    this.uncompressedSize = uncompressedSize;
  }

  public int getCompressedSize() {
    return compressedSize;
  }

 /**
  * @return the uncompressed size of the page when the bytes are compressed
  */
  public int getUncompressedSize() {
    return uncompressedSize;
  }

}
