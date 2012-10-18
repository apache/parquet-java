package redelm.pig;

import java.util.Arrays;

public class ColumnData {

  private final String[] path;
  private byte[] data;

  public ColumnData(String[] path, byte[] data) {
    super();
    this.path = path;
    this.data = data;
  }

  public String[] getPath() {
    return path;
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public String toString() {
    return "ColumnData{"+Arrays.toString(path)+" " +  data.length + "B}";
  }
}
