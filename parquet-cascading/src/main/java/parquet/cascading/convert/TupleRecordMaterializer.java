package parquet.cascading.convert;

import cascading.tuple.Tuple;
import cascading.tuple.Fields;

import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;

public class TupleRecordMaterializer extends RecordMaterializer<Tuple> {

  private TupleConverter root;

  public TupleRecordMaterializer(GroupType parquetSchema) {
    this.root = new TupleConverter(parquetSchema);
  }

  @Override
  public Tuple getCurrentRecord() {
    return root.getCurrentTuple();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }

}