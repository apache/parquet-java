package parquet.io.convert;

abstract public class RecordConverter<T> extends GroupConverter {

  abstract public T getCurrentRecord();

}
