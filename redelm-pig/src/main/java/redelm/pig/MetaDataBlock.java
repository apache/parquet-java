package redelm.pig;

public class MetaDataBlock {

  private final String name;
  private final byte[] data;

  public MetaDataBlock(String name, byte[] data) {
    this.name = name;
    this.data = data;

  }

  public String getName() {
    return name;
  }

  public byte[] getData() {
    return data;
  }

}
