package parquet.pig.convert;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import parquet.io.convert.GroupConverter;
import parquet.io.convert.RecordConverter;
import parquet.schema.GroupType;

public class TupleRecordConverter extends RecordConverter<Tuple> {

  private TupleConverter root;

  public TupleRecordConverter(GroupType parquetSchema, Schema pigSchema) {
    this.root = new TupleConverter(parquetSchema, pigSchema);
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
