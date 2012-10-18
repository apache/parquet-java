package redelm.pig;

import java.util.Arrays;

public class ColumnData {

  private final String[] path;
  private byte[] repetitionLevels;
  private byte[] definitionLevels;
  private byte[] data;

  public ColumnData(String[] path,
      byte[] repetitionLevels, byte[] definitionLevels, byte[] data) {
    super();
    this.path = path;
    this.repetitionLevels = repetitionLevels;
    this.definitionLevels = definitionLevels;
    this.data = data;
  }

  public String[] getPath() {
    return path;
  }

  public byte[] getRepetitionLevels() {
    return repetitionLevels;
  }

  public byte[] getDefinitionLevels() {
    return definitionLevels;
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public String toString() {
    return "ColumnData{"+Arrays.toString(path) + " "
        +  repetitionLevels.length + "B "
        +  data.length + "B "
        +  data.length + "B}";
  }
}
