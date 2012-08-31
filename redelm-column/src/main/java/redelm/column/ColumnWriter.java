package redelm.column;

public interface ColumnWriter {

  void write(int value, int repetitionLevel, int definitionLevel);

  void write(String value, int repetitionLevel, int definitionLevel);

  void write(boolean value, int repetitionLevel, int definitionLevel);

  void write(byte[] value, int repetitionLevel, int definitionLevel);

  void writeNull(int repetitionLevel, int definitionLevel);

}
