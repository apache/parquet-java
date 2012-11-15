package redelm.hadoop;

/**
 * Raw content of a Metadata block in the file
 *
 * @author Julien Le Dem
 *
 */
public class MetaDataBlock {

  private final String name;
  private final byte[] data;

  /**
   *
   * @param name name of the block (must be unique)
   * @param data data for the block
   */
  public MetaDataBlock(String name, byte[] data) {
    this.name = name;
    this.data = data;
  }

  /**
   *
   * @return name of the block
   */
  public String getName() {
    return name;
  }

  /**
   *
   * @return raw content
   */
  public byte[] getData() {
    return data;
  }

}
