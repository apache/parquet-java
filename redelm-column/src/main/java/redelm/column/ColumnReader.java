package redelm.column;


public interface ColumnReader {

  boolean isFullyConsumed();

  void consume();

  /**
   * must return 0 when isFullyConsumed() == true
   * @return
   */
  int getCurrentRepetitionLevel();

  int getCurrentDefinitionLevel();

  String getString();

  int getInt();

  boolean getBool();

  byte[] getBinary();

}
