package parquet.column.page;

public interface PageVisitor {

  void visit(DataPage dataPage);

  void visit(DictionaryPage dictionaryPage);

  void visit(StatsPage statsPage);

}
