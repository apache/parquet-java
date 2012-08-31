package redelm.column;


public interface ColumnReader {

  boolean isFullyConsumed();

  void consume();

  int getCurrentRepetitionLevel();

  int getCurrentDefinitionLevel();

  String getString();

  int getInt();

  boolean getBool();

  byte[] getBinary();

}
